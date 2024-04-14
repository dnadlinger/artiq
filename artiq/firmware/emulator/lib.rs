#![feature(
    libc,
    panic_unwind,
    unwind_attributes,
    rustc_private,
    int_bits_const,
    const_in_array_repeat_expressions,
    format_args_capture
)]
#![crate_name = "artiq_emulator"]
#![crate_type = "cdylib"]

extern crate byteorder;
extern crate failure;
extern crate libc;
#[macro_use]
extern crate failure_derive;
extern crate std as core;
use std as alloc;
extern crate io;
extern crate unwind;

#[path = "."]
pub mod eh {
    #[path = "../libeh/dwarf.rs"]
    pub mod dwarf;
    #[path = "../libeh/eh_artiq.rs"]
    pub mod eh_artiq;
}
#[path = "../ksupport/eh_artiq.rs"]
pub mod eh_artiq;

#[path = "."]
pub mod proto_artiq {
    #[path = "../libproto_artiq/rpc_proto.rs"]
    pub mod rpc_proto;
    #[path = "../libproto_artiq/session_proto.rs"]
    pub mod session_proto;
}

use alloc::eprintln;
use alloc::option::Option::Some;
use proto_artiq::rpc_proto as rpc;
use proto_artiq::session_proto as host;

// Note: this does *not* match the cslice crate!
// ARTIQ Python has the slice length field fixed at 32 bits, even on 64-bit platforms.
mod cslice {
    use core::convert::AsRef;
    use core::marker::PhantomData;
    use core::slice;

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct CSlice<'a, T> {
        base: *const T,
        len: u32,
        phantom: PhantomData<&'a ()>,
    }

    impl<'a, T> CSlice<'a, T> {
        pub unsafe fn new(base: *const T, len: usize) -> Self {
            assert!(base != std::ptr::null_mut());
            CSlice {
                base: base,
                len: len as u32,
                phantom: PhantomData,
            }
        }

        pub fn len(&self) -> usize {
            self.len as usize
        }

        pub fn as_ptr(&self) -> *const T {
            self.base
        }
    }

    impl<'a, T> AsRef<[T]> for CSlice<'a, T> {
        fn as_ref(&self) -> &[T] {
            unsafe { slice::from_raw_parts(self.base, self.len as usize) }
        }
    }

    pub trait AsCSlice<'a, T> {
        fn as_c_slice(&'a self) -> CSlice<'a, T>;
    }

    impl<'a> AsCSlice<'a, u8> for str {
        fn as_c_slice(&'a self) -> CSlice<'a, u8> {
            CSlice {
                base: self.as_ptr(),
                len: self.len() as u32,
                phantom: PhantomData,
            }
        }
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct CMutSlice<'a, T> {
        base: *mut T,
        len: u32,
        phantom: PhantomData<&'a ()>,
    }

    impl<'a, T> CMutSlice<'a, T> {
        pub unsafe fn new(base: *mut T, len: usize) -> Self {
            assert!(base != std::ptr::null_mut());
            CMutSlice {
                base: base,
                len: len as u32,
                phantom: PhantomData,
            }
        }
    }

    impl<'a, T> AsRef<[T]> for CMutSlice<'a, T> {
        fn as_ref(&self) -> &[T] {
            unsafe { slice::from_raw_parts(self.base, self.len as usize) }
        }
    }

    impl<'a, T> AsMut<[T]> for CMutSlice<'a, T> {
        fn as_mut(&mut self) -> &mut [T] {
            unsafe { slice::from_raw_parts_mut(self.base, self.len as usize) }
        }
    }
}

use core::convert::AsRef;
use std::{process, str};

fn terminate(
    exceptions: &'static [Option<eh_artiq::Exception<'static>>],
    _stack_pointers: &'static [eh_artiq::StackPointerBacktrace],
    _backtrace: &'static mut [(usize, usize)],
) -> ! {
    eprintln!("{}", exceptions.len());
    for exception in exceptions.iter() {
        let exception = exception.as_ref().unwrap();
        eprintln!(
            "Uncaught {}: {} ({}, {}, {})",
            exception.id,
            str::from_utf8(exception.message.as_ref()).unwrap(),
            exception.param[0],
            exception.param[1],
            exception.param[2]
        );
        eprintln!(
            "at {}:{}:{}",
            str::from_utf8(exception.file.as_ref()).unwrap(),
            exception.line,
            exception.column
        );
    }
    process::exit(1);
}

#[export_name = "now"]
pub static mut NOW: i64 = 0;

extern "C" {
    fn __modinit__();
}

use cslice::CSlice;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;

// Not using kernel_proto, as this also pulls in dyld (and also weirdly mixed requests
// and replies in one type).

#[derive(Debug)]
pub enum KernelToWorker<'a> {
    RpcSend {
        is_async: bool,
        service: u32,
        tag: &'a [u8],
        data: *const *const (),
    },
    RpcRecv(*mut ()),
    // Log(fmt::Arguments<'a>),
    // LogSlice(&'a str),
    KernelFinished,
}

#[derive(Debug)]
pub enum WorkerToKernel<'a> {
    RpcRecv(Result<u32, eh::eh_artiq::Exception<'a>>),
    RpcFlush,
}
// HACK: To use existing types such as eh::eh_artiq::Exception, just throw away type
// system assurances about sharing. Will need to carefully validate threading
// assumptions regarding the main kernel thread and comms thread, but in the first
// instance, the synchronisation given by the message exchange seems like it should
// be enough.
unsafe impl<'a> Send for KernelToWorker<'a> {}
unsafe impl<'a> Send for WorkerToKernel<'a> {}

static mut TO_WORKER_TX: Option<mpsc::Sender<KernelToWorker>> = None;
static mut FROM_WORKER_RX: Option<mpsc::Receiver<WorkerToKernel>> = None;

static mut WORKER: Option<TcpStream> = None;

struct Worker;
impl io::Read for Worker {
    type ReadError = std::io::Error;

    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::ReadError> {
        unsafe { WORKER.as_mut().unwrap().read(buf) }
    }
}
impl io::Write for Worker {
    type WriteError = std::io::Error;
    type FlushError = std::io::Error;
    fn write(&mut self, buf: &[u8]) -> Result<usize, Self::WriteError> {
        eprintln!("write(): {buf:?}");
        unsafe { WORKER.as_mut().unwrap().write(buf) }
    }

    fn flush(&mut self) -> Result<(), Self::FlushError> {
        eprintln!("flush()");
        unsafe { WORKER.as_mut().unwrap().flush() }
    }
}

#[no_mangle]
extern "C" fn rpc_recv(slot: *mut ()) -> u32 {
    let tx_queue = unsafe { TO_WORKER_TX.as_mut().unwrap() };
    tx_queue.send(KernelToWorker::RpcRecv(slot)).unwrap();
    let rx_queue = unsafe { FROM_WORKER_RX.as_mut().unwrap() };
    let reply = rx_queue.recv().unwrap();
    if let WorkerToKernel::RpcRecv(Ok(size)) = reply {
        size
    } else {
        panic!("expected RpcRecv, not {:?}", reply)
    }
}

#[no_mangle]
extern "C" fn rpc_send(service: u32, tag: &CSlice<u8>, data: *const *const ()) -> () {
    let queue = unsafe { TO_WORKER_TX.as_mut().unwrap() };
    queue
        .send(KernelToWorker::RpcSend {
            is_async: false,
            service,
            tag: unsafe { std::mem::transmute(tag.as_ref()) },
            data,
        })
        .unwrap();
}

#[no_mangle]
extern "C" fn rpc_send_async(service: u32, tag: &CSlice<u8>, data: *const *const ()) -> () {
    let queue = unsafe { TO_WORKER_TX.as_mut().unwrap() };
    queue
        .send(KernelToWorker::RpcSend {
            is_async: true,
            service,
            tag: unsafe { std::mem::transmute(tag.as_ref()) },
            data,
        })
        .unwrap();
}

fn listen_and_accept_worker() -> std::io::Result<TcpStream> {
    let host = "127.0.0.1";
    let listener = TcpListener::bind(format!("{host}:0"))?;
    let addr = listener.local_addr()?;
    let port = addr.port();

    // Print banner as expected by CommKernelEmulator.
    println!("ARTIQ kernel emulator, listening on: {}:{}", host, port);
    eprintln!("ARTIQ kernel emulator, listening on: {}:{}", host, port);

    let (socket, _addr) = listener.accept()?;
    Ok(socket)
}

#[no_mangle]
pub unsafe fn main() -> std::io::Result<()> {
    WORKER = listen_and_accept_worker()?.into();

    let request = host::Request::read_from(&mut Worker {}).unwrap();
    match request {
        host::Request::RunKernel => (),
        _ => panic!("unexpected worker message: {:?}", request),
    }

    let (to_worker_tx, to_worker_rx) = mpsc::channel();
    TO_WORKER_TX = Some(to_worker_tx);
    let (from_worker_tx, from_worker_rx) = mpsc::channel();
    FROM_WORKER_RX = Some(from_worker_rx);
    let comm_thead = thread::spawn(move || {
        loop {
            eprintln!(" -- Waiting for message to worker");
            let to_worker = to_worker_rx.recv().unwrap();
            eprintln!(" -- Got message to worker: {:?}", to_worker);
            match to_worker {
                KernelToWorker::RpcSend {
                    is_async,
                    service,
                    tag,
                    data,
                } => {
                    host::Reply::RpcRequest { async: is_async }
                        .write_to(&mut Worker {})
                        .unwrap();
                    rpc::send_args(&mut Worker {}, service, tag, data, true).unwrap();
                    if is_async {
                        continue;
                    }
                }
                KernelToWorker::KernelFinished => {
                    eprintln!("===== finished =====");
                    host::Reply::KernelFinished { async_errors: 0 }
                        .write_to(&mut Worker {})
                        .unwrap();
                    break;
                }
                _ => panic!("unexpected kernel message: {:?}", request),
            }

            let request = host::Request::read_from(&mut Worker {}).unwrap();
            eprintln!(" -- Received worker message: {:?}", request);
            match request {
                host::Request::RpcReply { tag } => {
                    let msg = to_worker_rx.recv().unwrap();
                    let root_slot = if let KernelToWorker::RpcRecv(slot) = msg {
                        slot
                    } else {
                        panic!("expected root value slot from kernel CPU, not {:?}", msg)
                    };
                    eprintln!(" -- Got msg: {:?}", msg);
                    rpc::recv_return(&mut Worker {}, &tag, root_slot, &|size| -> Result<
                        _,
                        io::Error<std::io::Error>,
                    > {
                        if size == 0 {
                            // Don't try to allocate zero-length values, as RpcRecvReply(0) is
                            // used to terminate the kernel-side receive loop.
                            return Ok(0 as *mut ());
                        }
                        from_worker_tx
                            .send(WorkerToKernel::RpcRecv(Ok(size as u32)))
                            .unwrap();
                        let reply = to_worker_rx.recv().unwrap();
                        if let KernelToWorker::RpcRecv(slot) = reply {
                            Ok(slot)
                        } else {
                            panic!("expected nested value slot from kernel CPU, not {:?}", msg)
                        }
                    })
                    .unwrap();
                    from_worker_tx.send(WorkerToKernel::RpcRecv(Ok(0))).unwrap();
                }
                _ => panic!("unexpected worker message: {:?}", request),
            }
        }
    });
    __modinit__();
    TO_WORKER_TX
        .as_ref()
        .unwrap()
        .send(KernelToWorker::KernelFinished)
        .unwrap();
    comm_thead.join().unwrap();
    Ok(())
}

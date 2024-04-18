#![feature(
    panic_unwind,
    unwind_attributes,
    rustc_private,
    int_bits_const,
    const_in_array_repeat_expressions,
    format_args_capture
)]
#![crate_name = "artiq_emulator"]
#![crate_type = "cdylib"]

mod bare_thread;

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
        pub unsafe fn new(base: *const T, len: u32) -> Self {
            assert!(base != std::ptr::null_mut());
            CSlice {
                base: base,
                len: len as u32,
                phantom: PhantomData,
            }
        }

        pub fn len(&self) -> u32 {
            self.len as u32
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

use cslice::CSlice;
use proto_artiq::rpc_proto as rpc;
use proto_artiq::session_proto as host;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;

fn terminate(
    exceptions: &'static [Option<eh_artiq::Exception<'static>>],
    stack_pointers: &'static [eh_artiq::StackPointerBacktrace],
    backtrace: &'static mut [(usize, usize)],
) -> ! {
    let queue = unsafe { TO_WORKER_TX.as_ref().unwrap() };

    queue
        .send(KernelToWorker::RunException {
            exceptions,
            stack_pointers,
            backtrace,
        })
        .unwrap();
    std::thread::park();
    panic!("should not have unparked thread after unhandled kernel exception");
}

#[export_name = "now"]
pub static mut NOW: i64 = 0;

extern "C" {
    fn __modinit__();
}

// Not using kernel_proto, as this also pulls in dyld (and also weirdly mixed requests
// and replies in one type).

#[derive(Debug)]
pub enum KernelToWorker<'a> {
    RpcSend {
        is_async: bool,
        buffer: Vec<u8>,
    },
    RpcRecv(*mut ()),
    // Log(fmt::Arguments<'a>),
    // LogSlice(&'a str),
    RunFinished,
    RunException {
        exceptions: &'a [Option<eh::eh_artiq::Exception<'a>>],
        stack_pointers: &'a [eh::eh_artiq::StackPointerBacktrace],
        backtrace: &'a [(usize, usize)],
    },
}
// HACK: To use existing types such as eh::eh_artiq::Exception, just throw away type
// system assurances about sharing. Will need to carefully validate threading
// assumptions regarding the main kernel thread and comms thread, but in the first
// instance, the synchronisation given by the message exchange seems like it should
// be enough.
unsafe impl<'a> Send for KernelToWorker<'a> {}

#[derive(Debug)]
struct HostException {
    id: u32,
    message: u32,
    param: [i64; 3],
    file: u32,
    line: u32,
    column: u32,
    function: u32,
}
#[derive(Debug)]
enum WorkerToKernel {
    RpcRecv(Result<u32, HostException>),
    RpcFlush,
}

static mut TO_WORKER_TX: Option<mpsc::Sender<KernelToWorker>> = None;
static mut FROM_WORKER_RX: Option<mpsc::Receiver<WorkerToKernel>> = None;

struct Worker(TcpStream);
impl io::Read for Worker {
    type ReadError = std::io::Error;

    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::ReadError> {
        self.0.read(buf)
    }
}
impl io::Write for Worker {
    type WriteError = std::io::Error;
    type FlushError = std::io::Error;
    fn write(&mut self, buf: &[u8]) -> Result<usize, Self::WriteError> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> Result<(), Self::FlushError> {
        self.0.flush()
    }
}

#[no_mangle]
extern "C" fn rpc_recv(slot: *mut ()) -> u32 {
    let tx_queue = unsafe { TO_WORKER_TX.as_mut().unwrap() };
    tx_queue.send(KernelToWorker::RpcRecv(slot)).unwrap();
    let rx_queue = unsafe { FROM_WORKER_RX.as_mut().unwrap() };
    let reply = rx_queue.recv().unwrap();
    match reply {
        WorkerToKernel::RpcRecv(Ok(alloc_size)) => alloc_size,
        WorkerToKernel::RpcRecv(Err(ref exn)) => unsafe {
            // message/file/function are not actually slices; usize::MAX acts as a
            // marker for them to be treated as host-side strings.
            eh_artiq::raise(&eh_artiq::Exception {
                id: exn.id,
                message: CSlice::new(exn.message as *const u8, u32::MAX),
                param: exn.param,
                file: CSlice::new(exn.file as *const u8, u32::MAX),
                line: exn.line,
                column: exn.column,
                function: CSlice::new(exn.function as *const u8, u32::MAX),
            })
        },
        _ => panic!("expected RpcRecv, not {:?}", reply),
    }
}

fn send_rpc(is_async: bool, service: u32, tag: &CSlice<u8>, data: *const *const ()) {
    let mut buffer: Vec<u8> = Vec::new();
    host::Reply::RpcRequest { async: is_async }
        .write_to(&mut buffer)
        .unwrap();
    rpc::send_args(&mut buffer, service, tag.as_ref(), data, true).unwrap();
    unsafe { TO_WORKER_TX.as_ref().unwrap() }
        .send(KernelToWorker::RpcSend { is_async, buffer })
        .unwrap();
}

#[no_mangle]
extern "C" fn rpc_send(service: u32, tag: &CSlice<u8>, data: *const *const ()) -> () {
    send_rpc(false, service, tag, data);
}

#[no_mangle]
extern "C" fn rpc_send_async(service: u32, tag: &CSlice<u8>, data: *const *const ()) -> () {
    send_rpc(true, service, tag, data);
}

fn listen_and_accept_worker() -> std::io::Result<TcpStream> {
    let host = "127.0.0.1";
    let listener = TcpListener::bind(format!("{host}:0"))?;
    let addr = listener.local_addr()?;
    let port = addr.port();

    // Print banner as expected by CommKernelEmulator.
    println!("ARTIQ kernel emulator, listening on: {}:{}", host, port);

    let (socket, _addr) = listener.accept()?;
    Ok(socket)
}

#[no_mangle]
pub unsafe fn main() -> std::io::Result<()> {
    let mut worker = Worker {
        0: listen_and_accept_worker()?,
    };

    let request = host::Request::read_from(&mut worker).unwrap();
    match request {
        host::Request::RunKernel => (),
        _ => panic!("unexpected worker message: {:?}", request),
    }

    // Queues for communication between the kernel and main (socket comms) thread. This
    // could just be SPSC, but std::sync::mpsc exists and is easy to use; good for now.
    let (to_worker_tx, to_worker_rx) = mpsc::channel();
    TO_WORKER_TX = Some(to_worker_tx);
    let (from_worker_tx, from_worker_rx) = mpsc::channel();
    FROM_WORKER_RX = Some(from_worker_rx);

    // We need to run the kernel code and socket handling in separate threads to be able
    // to make use of the existing session_proto implementations, which use blocking
    // waits for the RPC recv slots on the kernel side.
    //
    // Furthermore, the kernel needs to run on a (detached) background thread, as we
    // cannot actually handle uncaught kernel exceptions from Rust; none of the panic
    // handling functions can deal with foreign exceptions. Thus, we just use the
    // eh_artiq terminate handler to fire off a message to the host and then park the
    // thread until the main thread exits. For this reason, we also need to directly
    // spawn an OS thread, as thread::spawn tries to catch panics to convert them into
    // a Result, which would also die in the attempt to catch a foreign exception.
    bare_thread::Thread::new(0, Box::new(move || {
        __modinit__();
        TO_WORKER_TX
            .as_ref()
            .unwrap()
            .send(KernelToWorker::RunFinished)
            .unwrap();
    }))
    .unwrap();

    loop {
        // eprintln!(" -- Waiting for message to worker");
        let to_worker = to_worker_rx.recv().unwrap();
        // eprintln!(" -- Got message to worker: {:?}", to_worker);
        match to_worker {
            KernelToWorker::RpcSend { is_async, buffer } => {
                worker.0.write_all(&buffer).unwrap();

                if is_async {
                    continue;
                }
            }
            KernelToWorker::RunFinished => {
                // eprintln!("===== finished cleanly =====");
                host::Reply::KernelFinished { async_errors: 0 }
                    .write_to(&mut worker)
                    .unwrap();
                break;
            }
            KernelToWorker::RunException {
                exceptions,
                stack_pointers,
                backtrace,
            } => {
                // eprintln!("===== finished with exception =====");
                let msg = host::Reply::KernelException {
                    exceptions,
                    stack_pointers,
                    backtrace,
                    async_errors: 0,
                };
                msg.write_to(&mut worker).unwrap();
                break;
            }
            _ => panic!("unexpected kernel message: {:?}", request),
        }

        let request = host::Request::read_from(&mut worker).unwrap();
        // eprintln!(" -- Received worker message: {:?}", request);
        match request {
            host::Request::RpcReply { tag } => {
                let msg = to_worker_rx.recv().unwrap();
                let root_slot = if let KernelToWorker::RpcRecv(slot) = msg {
                    slot
                } else {
                    panic!("expected root value slot from kernel thread, not {:?}", msg)
                };
                // eprintln!(" -- Got msg: {:?}", msg);
                rpc::recv_return(&mut worker, &tag, root_slot, &|size| -> Result<
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
                        panic!(
                            "expected nested value slot from kernel thread, not {:?}",
                            msg
                        )
                    }
                })
                .unwrap();
                from_worker_tx.send(WorkerToKernel::RpcRecv(Ok(0))).unwrap();
            }
            host::Request::RpcException {
                id,
                message,
                param,
                file,
                line,
                column,
                function,
            } => {
                let msg = to_worker_rx.recv().unwrap();
                let _root_slot = if let KernelToWorker::RpcRecv(slot) = msg {
                    slot
                } else {
                    panic!(
                        "expected (ignored) root value slot from kernel thread, not {:?}",
                        msg
                    )
                };

                from_worker_tx
                    .send(WorkerToKernel::RpcRecv(Err(HostException {
                        id,
                        message,
                        param,
                        file,
                        line,
                        column,
                        function,
                    })))
                    .unwrap();
            }

            _ => panic!("unexpected worker message: {:?}", request),
        }
    }

    Ok(())
}

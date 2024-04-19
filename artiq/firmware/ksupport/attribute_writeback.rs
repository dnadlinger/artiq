use cslice::CSlice;
use rpc_send_async;

struct Attr {
    offset: usize,
    tag:    CSlice<'static, u8>,
    name:   CSlice<'static, u8>
}

struct Type {
    attributes: *const *const Attr,
    objects:    *const *const ()
}

pub unsafe fn send_async_rpcs(typeinfo: *const ()) {
    let mut tys = typeinfo as *const *const Type;
    while !(*tys).is_null() {
        let ty = *tys;
        tys = tys.offset(1);

        let mut objects = (*ty).objects;
        while !(*objects).is_null() {
            let object = *objects;
            objects = objects.offset(1);

            let mut attributes = (*ty).attributes;
            while !(*attributes).is_null() {
                let attribute = *attributes;
                attributes = attributes.offset(1);

                if (*attribute).tag.len() > 0 {
                    rpc_send_async(0, &(*attribute).tag, [
                        &object as *const _ as *const (),
                        &(*attribute).name as *const _ as *const (),
                        (object as usize + (*attribute).offset) as *const ()
                    ].as_ptr());
                }
            }
        }
    }
}

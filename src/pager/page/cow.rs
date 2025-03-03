use crate::pager::buffer::IPageBuffer;

use super::{MutPage, RefPage};


pub enum CowPage<'src, 'dest, DestBuffer> where 'dest: 'src, DestBuffer: IPageBuffer {
    Borrowed {
        src: RefPage<'src>,
        dest_buffer: &'dest DestBuffer
    },
    Owned(MutPage<'dest>)
}



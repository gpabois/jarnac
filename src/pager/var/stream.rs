use std::io::{Cursor, Read};

use crate::pager::{page::{AsRefPageSlice, RefPage}, IPager, PagerResult};

use super::{SpillPage, Var};

/// Un flux en lecture sur une donnée de taille variable.
pub struct VarReader<'pager, Slice: AsRefPageSlice + 'pager, Pager: IPager> {
    pager: &'pager Pager,
    state: VarReaderState<'pager, Slice>
}

impl<'pager, Slice: AsRefPageSlice + 'pager, Pager: IPager> VarReader<'pager, Slice, Pager> {
    pub fn new(var: Var<Slice>, pager: &'pager Pager) -> Self {
        Self {
            pager,
            state: VarReaderState::InPage(Cursor::new(var))
        }
    }
}

impl<'pager, Slice: AsRefPageSlice + 'pager, Pager: IPager> Read for VarReader<'pager, Slice, Pager> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut read: usize = 0;

        while !self.state.is_eos() {
            read += self.state.read(buf)?;

            if self.state.is_exhausted() {
                self.state.next(self.pager).unwrap();
            } else {
                return Ok(read)
            }
        }

        Ok(read)
    }
}

enum VarReaderState<'pager, Slice: AsRefPageSlice + 'pager> {
    InPage(Cursor<Var<Slice>>),
    SpillPage(Cursor<SpillPage<RefPage<'pager>>>),
    EOS
}

impl<'pager, Slice: AsRefPageSlice + 'pager> Read for VarReaderState<'pager, Slice> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            VarReaderState::InPage(cursor) => cursor.read(buf),
            VarReaderState::SpillPage(cursor) => cursor.read(buf),
            VarReaderState::EOS => Ok(0),
        }
    }
}

impl<'pager, Slice: AsRefPageSlice + 'pager> VarReaderState<'pager, Slice> {
    pub fn is_exhausted(&self) -> bool {
        match self {
            VarReaderState::InPage(cursor) => cursor.position() >= cursor.get_ref().as_data().header.in_page_size,
            VarReaderState::SpillPage(cursor) => cursor.position() >= cursor.get_ref().as_data().in_page_size,
            VarReaderState::EOS => true,
        }
    }

    pub fn is_eos(&self) -> bool {
        matches!(self, Self::EOS)
    }

    pub fn next<Pager: IPager>(&mut self, pager: &'pager Pager) -> PagerResult<()> {
        match self {
            VarReaderState::InPage(cursor) => {
                *self = cursor.get_ref().as_data()
                    .header.spill_page_id.as_ref()
                    .map(|pid| pager.borrow_page(&pid).and_then(SpillPage::try_from).unwrap())
                    .map(Cursor::new)
                    .map(VarReaderState::SpillPage)
                    .unwrap_or(VarReaderState::EOS);

                Ok(())
            },
            VarReaderState::SpillPage(cursor) => {
                *self = cursor.get_ref().as_data().get_next()                    
                .map(|pid| pager.borrow_page(&pid).and_then(SpillPage::try_from).unwrap())
                .map(Cursor::new)
                .map(VarReaderState::SpillPage)
                .unwrap_or(VarReaderState::EOS);

                Ok(())
            },
            VarReaderState::EOS => Ok(()),
        }
    }
}

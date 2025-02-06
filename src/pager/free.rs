use std::io::Cursor;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::{
    page::{PageId, PageKind},
    IPagerInternals, PagerResult,
};

/// Représente les données stockée dans une page libre.
pub struct FreePage {
    next: Option<PageId>,
}

impl FreePage {
    pub fn new(next: Option<PageId>) -> Self {
        Self { next }
    }

    /// Lit les données d'une page libre.
    ///
    /// L'opération peut échouer si :
    /// - le type de la page n'est pas *free* ;
    /// - le slice est d'une taille insuffisante.
    pub fn read(page: &[u8]) -> PagerResult<Self> {
        let mut cursor = Cursor::new(page);
        let kind: PageKind = cursor.read_u8()?.try_into()?;
        kind.assert(PageKind::Free)?;

        let next = if cursor.read_u8()? == 1 {
            Some(cursor.read_u64::<LittleEndian>()?)
        } else {
            None
        };

        Ok(Self { next })
    }

    /// Lit les données depuis une page
    ///
    /// L'opération peut échouer si :
    /// - le slice est d'une taille insuffisante pour stocker les données.
    pub fn write(&self, page: &mut [u8]) -> PagerResult<()> {
        let mut cursor = Cursor::new(page);
        cursor.write_u8(PageKind::Free as u8)?;

        if let Some(next) = self.next {
            cursor.write_u8(1)?;
            cursor.write_u64::<LittleEndian>(next)?;
        } else {
            cursor.write_u8(0)?;
            cursor.write_u64::<LittleEndian>(0)?;
        }

        Ok(())
    }
}

/// Empile une nouvelle page libre dans la liste chaînée
pub fn push_free_page<Pager: IPagerInternals>(pager: &Pager, pid: &PageId) -> PagerResult<()> {
    let mut page = pager.get_mut_page(pid)?;
    page.fill(0);

    let free = FreePage::new(pager.free_head());
    free.write(&mut page)?;
    drop(page);

    pager.set_free_head(Some(*pid));

    Ok(())
}

/// Dépile une page libre dans la liste chaînée
pub fn pop_free_page<Pager: IPagerInternals>(pager: &Pager) -> PagerResult<Option<PageId>> {
    if let Some(next) = pager.free_head() {
        let new_head = FreePage::read(&pager.get_page(&next)?)?.next;
        pager.set_free_head(new_head);
        return Ok(Some(next));
    }

    return Ok(None);
}


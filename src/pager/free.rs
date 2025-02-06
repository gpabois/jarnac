use std::{
    io::{Cursor, Seek},
    ops::{Deref, DerefMut},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::{
    page::{PageId, PageKind},
    IPagerInternals, PagerResult,
};

/// Représente les données stockée dans une page libre.
pub struct FreePage<Page> {
    next: Option<PageId>,
    page: Page,
}

impl<P> FreePage<P> {}

impl<P> FreePage<P>
where
    P: Deref<Target = [u8]>,
{
    /// Lit les données d'une page libre.
    ///
    /// L'opération peut échouer si :
    /// - le type de la page n'est pas *free* ;
    /// - le slice est d'une taille insuffisante.
    pub fn load(page: P) -> PagerResult<Self> {
        let mut cursor = Cursor::new(page.deref());
        let kind: PageKind = cursor.read_u8()?.try_into()?;
        kind.assert(PageKind::Free)?;

        let next = if cursor.read_u8()? == 1 {
            Some(cursor.read_u64::<LittleEndian>()?)
        } else {
            None
        };

        Ok(Self { page, next })
    }
}

impl<P> FreePage<P>
where
    P: DerefMut<Target = [u8]>,
{
    pub fn new(page: P, next: Option<PageId>) -> std::io::Result<Self> {
        let mut free = Self { next, page };
        free.initialise()?;
        Ok(free)
    }
    /// Lit les données depuis une page
    ///
    /// L'opération peut échouer si :
    /// - le slice est d'une taille insuffisante pour stocker les données.
    fn initialise(&mut self) -> std::io::Result<()> {
        self.page.deref_mut().fill(0);
        let mut cursor = Cursor::new(self.page.deref_mut());
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

    pub fn set_next(&mut self, next: Option<PageId>) -> std::io::Result<()> {
        let mut cursor = Cursor::new(self.page.deref_mut());

        cursor.seek(std::io::SeekFrom::Start(1))?;
        if let Some(next) = next {
            cursor.write_u8(1)?;
            cursor.write_u64::<LittleEndian>(next)?;
        } else {
            cursor.write_u8(0)?;
            cursor.write_u64::<LittleEndian>(0)?;
        }

        self.next = next;

        Ok(())
    }
}

/// Empile une nouvelle page libre dans la liste chaînée
pub(super) fn push_free_page<Pager: IPagerInternals>(
    pager: &Pager,
    pid: &PageId,
) -> PagerResult<()> {
    let mut page = pager.get_mut_page(pid)?;
    page.fill(0);

    FreePage::new(page, pager.free_head())?;

    pager.set_free_head(Some(*pid));

    Ok(())
}

/// Dépile une page libre dans la liste chaînée
pub(super) fn pop_free_page<Pager: IPagerInternals>(pager: &Pager) -> PagerResult<Option<PageId>> {
    if let Some(next) = pager.free_head() {
        let new_head = FreePage::load(pager.get_page(&next)?)?.next;
        pager.set_free_head(new_head);
        return Ok(Some(next));
    }

    Ok(None)
}


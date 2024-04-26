use std::{
    fs::File,
    io::{Read, Seek},
    path::Path,
};

use log::info;

use crate::tree::LeafNode;

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;

pub struct Pager {
    pub file_descriptor: File,
    pub file_length: u64,
    pub num_pages: u32,
    pub pages: Vec<Option<Box<LeafNode>>>,
}

impl Pager {
    pub fn open_file(file_path: String) -> Result<Pager, &'static str> {
        // check if file exists
        let file_exists = Path::new(&file_path).exists();

        if file_exists {
            match File::options()
                .read(true)
                .write(true)
                .open(file_path.as_str())
            {
                Ok(file) => {
                    let meta = file.metadata().unwrap();
                    let pages: Vec<Option<Box<LeafNode>>> = vec![None; TABLE_MAX_PAGES];
                    let file_length = meta.len();

                    if file_length % PAGE_SIZE as u64 != 0 {
                        return Err("Db file length is not a valid number of pages. Corrupt file");
                    }

                    return Ok(Pager {
                        file_descriptor: file,
                        file_length,
                        num_pages: (file_length as usize / PAGE_SIZE) as u32,
                        pages,
                    });
                }
                Err(_) => return Err("Error opening file"),
            }
        } else {
            let file = File::create_new(file_path).unwrap();
            let meta = file.metadata().unwrap();
            let pages: Vec<Option<Box<LeafNode>>> = vec![None; TABLE_MAX_PAGES];

            return Ok(Pager {
                file_descriptor: file,
                file_length: meta.len(),
                num_pages: 0,
                pages,
            });
        }
    }

    pub fn get_page<'a>(&'a mut self, page_num: usize) -> Result<&'a mut LeafNode, &'a str> {
        if page_num > TABLE_MAX_PAGES {
            return Err("Hit page limit for table");
        }

        if self.pages[page_num].is_none() {
            let mut new_node = Box::new(LeafNode::new());
            let mut raw_data = [0u8; PAGE_SIZE];
            let file_pages = self.file_length as usize / PAGE_SIZE;

            info!(
                "trying to fetch page {}, file_pages = {}, divided = {}",
                page_num,
                file_pages,
                self.file_length as usize / PAGE_SIZE
            );

            if page_num < file_pages {
                match self
                    .file_descriptor
                    .seek(std::io::SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                {
                    Ok(_) => {
                        // save buffer in pages

                        self.file_descriptor.read_exact(&mut raw_data).unwrap();
                    }
                    Err(_) => return Err("Error trying to reach page from file"),
                }
            }

            LeafNode::serialize_node(raw_data.as_mut_ptr(), &mut new_node);

            self.pages[page_num] = Some(new_node);
        }

        match self.pages[page_num] {
            Some(ref mut page) => {
                return Ok(page);
            }
            None => {
                return Err("Error fetching page");
            }
        };

        // // check if we already loaded this page
        // match self.pages[page_num] {
        //     Some(page) => {
        //         return Ok(page);
        //     }
        //     None => {
        //         let allocated: Box<LeafNode> = Box::new(LeafNode::new());

        //         // check if the file has enough data
        //         // let file_pages = self.file_length as usize / PAGE_SIZE;

        //         // info!(
        //         //     "trying to fetch page {}, file_pages = {}, divided = {}",
        //         //     page_num,
        //         //     file_pages,
        //         //     self.file_length as usize / PAGE_SIZE
        //         // );

        //         // if page_num < file_pages {
        //         //     match self
        //         //         .file_descriptor
        //         //         .seek(std::io::SeekFrom::Start((page_num * PAGE_SIZE) as u64))
        //         //     {
        //         //         Ok(_) => {
        //         //             // save buffer in pages

        //         //             self.file_descriptor.read_exact(&mut allocated).unwrap();
        //         //         }
        //         //         Err(_) => return Err("Error trying to reach page from file"),
        //         //     }
        //         // }

        //         return Ok(allocated);
        //     }
        // }
    }
}

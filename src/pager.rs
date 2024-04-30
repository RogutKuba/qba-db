use std::{
    fs::File,
    io::{Read, Seek},
    path::Path,
};

use log::info;

use crate::leaf_node::LeafNode;

pub const PAGE_SIZE: usize = 150;
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

    pub fn get_page(&mut self, page_num: usize) -> Result<&mut LeafNode, &str> {
        if page_num > TABLE_MAX_PAGES {
            return Err("Hit page limit for table");
        }

        self.check_page(page_num).unwrap();

        match &mut self.pages[page_num] {
            Some(page) => {
                return Ok(page);
            }
            None => {
                return Err("Error fetching page");
            }
        };
    }

    pub fn get_two_pages(
        &mut self,
        first_page_num: usize,
        second_page_num: usize,
    ) -> Result<(&mut LeafNode, &mut LeafNode), &str> {
        if first_page_num > TABLE_MAX_PAGES || second_page_num > TABLE_MAX_PAGES {
            return Err("Hit page limit for table");
        }

        self.check_page(first_page_num).unwrap();
        self.check_page(second_page_num).unwrap();

        let (lower, higher) = {
            if first_page_num < second_page_num {
                (first_page_num, second_page_num)
            } else {
                (second_page_num, first_page_num)
            }
        };

        let (a, b) = self.pages.split_at_mut(higher);

        // Get mutable references to the page contents, handling cases where they might be None
        let lower_page_ref = match a[lower].as_mut() {
            Some(page) => page,
            None => return Err("Requested page does not exist"),
        };
        let higher_page_ref = match b[0].as_mut() {
            Some(page) => page,
            None => return Err("Requested page does not exist"),
        };

        if first_page_num == lower {
            Ok((lower_page_ref, higher_page_ref))
        } else {
            Ok((higher_page_ref, lower_page_ref))
        }
    }

    fn check_page(&mut self, page_num: usize) -> Result<(), &str> {
        if self.pages[page_num].is_none() {
            info!("adding new page at index {}", page_num);
            let mut new_node = Box::new(LeafNode::new());
            let mut raw_data = [0u8; PAGE_SIZE];
            let file_pages = self.file_length as usize / PAGE_SIZE;

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
            self.num_pages = self.num_pages + 1;
        }

        Ok(())
    }

    pub fn get_unused_page_num(&self) -> u32 {
        return self.num_pages;
    }
}

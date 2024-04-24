use std::{
    fs::File,
    io::{Read, Seek},
    path::Path,
};

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;

pub struct Pager {
    pub file_descriptor: File,
    pub file_length: u64,
    pub pages: Vec<Option<Vec<u8>>>,
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
                    let pages: Vec<Option<Vec<u8>>> = vec![None; TABLE_MAX_PAGES];

                    return Ok(Pager {
                        file_descriptor: file,
                        file_length: meta.len(),
                        pages,
                    });
                }
                Err(_) => return Err("Error opening file"),
            }
        } else {
            let file = File::create_new(file_path).unwrap();
            let meta = file.metadata().unwrap();
            let pages: Vec<Option<Vec<u8>>> = vec![None; TABLE_MAX_PAGES];

            return Ok(Pager {
                file_descriptor: file,
                file_length: meta.len(),
                pages,
            });
        }
    }

    pub fn get_page(&mut self, page_num: usize) -> Result<*mut u8, &str> {
        if page_num > TABLE_MAX_PAGES {
            return Err("Hit page limit for table");
        }

        // check if we already loaded this page
        match self.pages.get_mut(page_num) {
            Some(page_opt) => {
                match page_opt {
                    Some(page) => return Ok(page.as_mut_ptr()),
                    None => {
                        // println!(
                        //     "Loading page {} from memory as was not cached {}",
                        //     page_num, PAGE_SIZE
                        // );
                        // need to load from memory
                        let mut allocated = vec![0u8; PAGE_SIZE];

                        // check if the file has enough data
                        let file_pages = self.file_length as usize / PAGE_SIZE;

                        // info!(
                        //     "trying to fetch page {}, file_pages = {}, divided = {}",
                        //     page_num,
                        //     file_pages,
                        //     self.file_length as usize / PAGE_SIZE
                        // );

                        if page_num < file_pages {
                            // println!("Page in inside file! loading from file");
                            match self
                                .file_descriptor
                                .seek(std::io::SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                            {
                                Ok(_) => {
                                    // save buffer in pages

                                    self.file_descriptor.read_exact(&mut allocated).unwrap();
                                }
                                Err(_) => return Err("Error trying to reach page from file"),
                            }
                        } else if page_num == file_pages {
                            // check for partial page

                            let partial_page_length =
                                (self.file_length % PAGE_SIZE as u64) as usize;

                            match self
                                .file_descriptor
                                .seek(std::io::SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                            {
                                Ok(_) => {
                                    // save buffer in pages
                                    self.file_descriptor
                                        .read_exact(&mut allocated[0..partial_page_length])
                                        .unwrap();
                                }
                                Err(_) => return Err("Error trying to reach page from file"),
                            }
                        }

                        self.pages[page_num] = Some(allocated);
                        return Ok(self.pages[page_num].as_mut().unwrap().as_mut_ptr());
                    }
                }
            }
            None => Err("Error fetching page from pager"),
        }
    }
}

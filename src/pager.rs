use std::{
    fs::File,
    io::{Read, Seek},
    path::Path,
};

use log::info;

use crate::{internal_node::InternalNode, leaf_node::LeafNode};

pub const PAGE_SIZE: usize = 150;
pub const TABLE_MAX_PAGES: usize = 100;

pub struct Pager {
    pub file_descriptor: File,
    pub file_length: u64,
    pub num_pages: u32,
    pub pages: Vec<(Option<Box<InternalNode>>, Option<Box<LeafNode>>)>,
}

pub enum NodeType {
    Leaf,
    Internal,
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
                    let mut pages: Vec<(Option<Box<InternalNode>>, Option<Box<LeafNode>>)> =
                        vec![(None, None); TABLE_MAX_PAGES];
                    let file_length = meta.len();

                    if file_length % PAGE_SIZE as u64 != 0 {
                        return Err("Db file length is not a valid number of pages. Corrupt file");
                    }

                    // if file is empty, init root node
                    if file_length == 0 {
                        let mut root_node = LeafNode::new();
                        root_node.is_root = true;

                        pages[0] = (None, Some(Box::new(root_node)));

                        return Ok(Pager {
                            file_descriptor: file,
                            file_length,
                            num_pages: 1,
                            pages,
                        });
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
            let mut pages: Vec<(Option<Box<InternalNode>>, Option<Box<LeafNode>>)> =
                vec![(None, None); TABLE_MAX_PAGES];

            let mut root_node = LeafNode::new();
            root_node.is_root = true;

            pages[0] = (None, Some(Box::new(root_node)));

            return Ok(Pager {
                file_descriptor: file,
                file_length: meta.len(),
                num_pages: 1,
                pages,
            });
        }
    }

    pub fn get_page_node_type(&mut self, page_num: usize) -> NodeType {
        if self.pages[page_num].0.is_some() {
            return NodeType::Internal;
        }

        if self.pages[page_num].1.is_some() {
            return NodeType::Leaf;
        }

        panic!("Trying to get node type for non-existent page!")
    }

    /*
    LEAF NODE METHODS
    */

    pub fn get_page_leaf(&mut self, page_num: usize) -> Result<&mut LeafNode, &str> {
        if page_num > TABLE_MAX_PAGES {
            return Err("Hit page limit for table");
        }

        // self.check_page_leaf(page_num).unwrap();

        match &mut self.pages[page_num].1 {
            Some(page) => {
                return Ok(page);
            }
            None => {
                return Err("Error fetching page! Leaf node does not exist at page_num");
            }
        };
    }

    pub fn ensure_page_leaf(&mut self, page_num: usize) -> Result<(), &str> {
        // check leaf node exists
        if self.pages[page_num].1.is_none() {
            // make sure we dont overwrite an internal node
            if self.pages[page_num].0.is_some() {
                return Err("Trying to check leaf node at page num where internal node exists");
            }

            info!("adding new page for leafnode at index {}", page_num);
            let mut new_node = Box::new(LeafNode::new());
            let file_pages = self.file_length as usize / PAGE_SIZE;

            if page_num < file_pages {
                let mut raw_data = [0u8; PAGE_SIZE];

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

                LeafNode::serialize_node(raw_data.as_mut_ptr(), &mut new_node);
            }

            self.pages[page_num] = (None, Some(new_node));
            self.num_pages = self.num_pages + 1;
        }
        Ok(())
    }

    pub fn get_two_pages_leaf(
        &mut self,
        first_page_num: usize,
        second_page_num: usize,
    ) -> Result<(&mut LeafNode, &mut LeafNode), &str> {
        if first_page_num > TABLE_MAX_PAGES || second_page_num > TABLE_MAX_PAGES {
            return Err("Hit page limit for table");
        }

        // self.check_page_leaf(first_page_num).unwrap();
        // self.check_page_leaf(second_page_num).unwrap();

        let (lower, higher) = {
            if first_page_num < second_page_num {
                (first_page_num, second_page_num)
            } else {
                (second_page_num, first_page_num)
            }
        };

        let (a, b) = self.pages.split_at_mut(higher);

        // Get mutable references to the page contents, handling cases where they might be None
        let lower_page_ref = match a[lower].1.as_mut() {
            Some(page) => page,
            None => return Err("Requested page does not exist"),
        };
        let higher_page_ref = match b[0].1.as_mut() {
            Some(page) => page,
            None => return Err("Requested page does not exist"),
        };

        if first_page_num == lower {
            Ok((lower_page_ref, higher_page_ref))
        } else {
            Ok((higher_page_ref, lower_page_ref))
        }
    }

    pub fn get_page_internal(&mut self, page_num: usize) -> Result<&mut InternalNode, &str> {
        if page_num > TABLE_MAX_PAGES {
            return Err("Hit page limit for table");
        }

        match &mut self.pages[page_num].0 {
            Some(page) => {
                return Ok(page);
            }
            None => {
                return Err("Error fetching page! Internal node does not exist at page_num");
            }
        };
    }

    pub fn get_unused_page_num(&self) -> u32 {
        return self.num_pages;
    }

    fn indent(level: u32) -> String {
        let mut res = String::from("");

        for _ in 0..level {
            res = res + "   ";
        }

        res
    }

    pub fn print_b_tree(&mut self, page_num: usize, indent_level: u32) {
        match self.get_page_node_type(page_num) {
            NodeType::Leaf => {
                let node = self.get_page_leaf(page_num).unwrap();

                let num_cells = node.num_cells;
                info!(
                    "{}- leaf (num_cells: {})",
                    Self::indent(indent_level),
                    num_cells
                );

                for i in 0..num_cells {
                    let cell_key = node.get_cell_key(i);
                    info!("{}- {}", Self::indent(indent_level), cell_key);
                }
            }
            NodeType::Internal => {
                let node = self.get_page_internal(page_num).unwrap();

                let num_keys = node.num_keys;
                info!(
                    "{}- internal (num_childs: {})",
                    Self::indent(indent_level),
                    num_keys
                );

                let mut child_nums: Vec<u32> = vec![];
                for i in 0..num_keys {
                    let child_num = node.get_child(i);
                    child_nums.push(child_num);
                }
                child_nums.push(node.right_child);

                for num in child_nums {
                    self.print_b_tree(num as usize, indent_level + 1);
                }
            }
        }
    }
}

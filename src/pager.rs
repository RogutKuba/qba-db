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
        if first_page_num == second_page_num {
            return Err("Tried to access same page num twice!");
        }

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

    pub fn get_internal_and_leaf(
        &mut self,
        internal_page_num: usize,
        leaf_page_num: usize,
    ) -> Result<(&mut InternalNode, &mut LeafNode), &str> {
        if internal_page_num == leaf_page_num {
            return Err("Tried to access same page num twice!");
        }

        if internal_page_num > TABLE_MAX_PAGES || leaf_page_num > TABLE_MAX_PAGES {
            return Err("Hit page limit for table");
        }

        let (lower, higher) = {
            if internal_page_num < leaf_page_num {
                (internal_page_num, leaf_page_num)
            } else {
                (leaf_page_num, internal_page_num)
            }
        };

        let (a, b) = self.pages.split_at_mut(higher);

        // Get mutable references to the page contents, handling cases where they might be None
        let lower_page_ref = match a[lower].0.as_mut() {
            Some(page) => page,
            None => return Err("Requested page does not exist"),
        };
        let higher_page_ref = match b[0].1.as_mut() {
            Some(page) => page,
            None => return Err("Requested page does not exist"),
        };

        Ok((lower_page_ref, higher_page_ref))
    }

    /**
     * RETURNS NODES FOR internal_node_insert
     * (parent, child, right_node of parent)
     */
    pub fn get_nodes_for_internal_node_insert(
        &mut self,
        parent_page_num: usize,
        child_page_num: usize,
    ) -> Result<(&mut InternalNode, &mut LeafNode, &mut Box<LeafNode>), &str> {
        if parent_page_num > TABLE_MAX_PAGES || child_page_num > TABLE_MAX_PAGES {
            return Err("Hit page limit for table");
        }

        let tmp_parent = self.get_page_internal(parent_page_num).unwrap();
        let right_child_page_num = tmp_parent.right_child as usize;
        let does_need_right_child = right_child_page_num != 0;

        info!(
            "parent: {}, child: {}, right: {}",
            parent_page_num, child_page_num, right_child_page_num
        );

        // now perform two split_at_muts to get all nodes
        if does_need_right_child {
            // ensure leaf nodes exist
            self.ensure_page_leaf(child_page_num).unwrap();
            self.ensure_page_leaf(right_child_page_num).unwrap();

            let (lower_idx, middle_idx, upper_idx) = if parent_page_num < child_page_num {
                if child_page_num < right_child_page_num {
                    (parent_page_num, child_page_num, right_child_page_num)
                } else if parent_page_num < right_child_page_num {
                    (parent_page_num, right_child_page_num, child_page_num)
                } else {
                    (right_child_page_num, parent_page_num, child_page_num)
                }
            } else {
                if parent_page_num < right_child_page_num {
                    (child_page_num, parent_page_num, right_child_page_num)
                } else if child_page_num < right_child_page_num {
                    (child_page_num, right_child_page_num, parent_page_num)
                } else {
                    (right_child_page_num, child_page_num, parent_page_num)
                }
            };

            // always 0
            let relative_middle_idx = 0;
            let relative_upper_idx = 0;

            let (lower, middle, upper) = {
                let (first, rest) = self.pages.split_at_mut(middle_idx);

                let (second, third) = rest.split_at_mut(upper_idx - middle_idx);

                (first, second, third)
            };

            // TODO: refactor below
            // parent node first
            if lower_idx == parent_page_num {
                let parent_node_ref = match lower[lower_idx].0.as_mut() {
                    Some(page) => page,
                    None => return Err("Requested page does not exist for parent 1"),
                };

                // if statement for child and right child is other
                if middle_idx == child_page_num {
                    let child_node_ref = match middle[relative_middle_idx].1.as_mut() {
                        Some(page) => page,
                        None => return Err("Requested page does not exist for mid 2"),
                    };

                    let right_node_ref = match upper[relative_upper_idx].1.as_mut() {
                        Some(page) => page,
                        None => return Err("Requested page does not exist for mid 3"),
                    };

                    return Ok((parent_node_ref, child_node_ref, right_node_ref));
                } else {
                    // upper == child
                    let child_node_ref = match upper[relative_upper_idx].1.as_mut() {
                        Some(page) => page,
                        None => return Err("Requested page does not exist for mid 4"),
                    };

                    let right_node_ref = match middle[relative_middle_idx].1.as_mut() {
                        Some(page) => page,
                        None => return Err("Requested page does not exist for mid 5"),
                    };

                    return Ok((parent_node_ref, child_node_ref, right_node_ref));
                }
            } else if middle_idx == parent_page_num {
                let parent_node_ref = match middle[relative_middle_idx].0.as_mut() {
                    Some(page) => page,
                    None => return Err("Requested page does not exist for parent 6"),
                };

                // if statement for child and right child is other
                if lower_idx == child_page_num {
                    let child_node_ref = match lower[lower_idx].1.as_mut() {
                        Some(page) => page,
                        None => return Err("Requested page does not exist for mid 7"),
                    };

                    let right_node_ref = match upper[relative_upper_idx].1.as_mut() {
                        Some(page) => page,
                        None => return Err("Requested page does not exist for mid 8"),
                    };

                    return Ok((parent_node_ref, child_node_ref, right_node_ref));
                } else {
                    // upper == child
                    let child_node_ref = match upper[relative_upper_idx].1.as_mut() {
                        Some(page) => page,
                        None => return Err("Requested page does not exist for mid 9"),
                    };

                    let right_node_ref = match lower[lower_idx].1.as_mut() {
                        Some(page) => page,
                        None => return Err("Requested page does not exist for mid 10"),
                    };

                    return Ok((parent_node_ref, child_node_ref, right_node_ref));
                }
            } else {
                let parent_node_ref = match upper[relative_upper_idx].0.as_mut() {
                    Some(page) => page,
                    None => return Err("Requested page does not exist for parent 11"),
                };

                // if statement for child and right child is other
                if lower_idx == child_page_num {
                    let child_node_ref = match lower[lower_idx].1.as_mut() {
                        Some(page) => page,
                        None => return Err("Requested page does not exist for mid 12"),
                    };

                    let right_node_ref = match middle[relative_middle_idx].1.as_mut() {
                        Some(page) => page,
                        None => return Err("Requested page does not exist for mid 13"),
                    };

                    return Ok((parent_node_ref, child_node_ref, right_node_ref));
                } else {
                    // middle == child
                    let child_node_ref = match middle[relative_middle_idx].1.as_mut() {
                        Some(page) => page,
                        None => return Err("Requested page does not exist for mid 14"),
                    };

                    let right_node_ref = match lower[lower_idx].1.as_mut() {
                        Some(page) => page,
                        None => return Err("Requested page does not exist for mid 15"),
                    };

                    return Ok((parent_node_ref, child_node_ref, right_node_ref));
                }
            };
        } else {
            panic!("How is right_child 0??");
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
            res = res + "\t";
        }

        res
    }

    pub fn print_b_tree(&mut self, page_num: usize, indent_level: u32) {
        match self.get_page_node_type(page_num) {
            NodeType::Leaf => {
                let node = self.get_page_leaf(page_num).unwrap();

                let num_cells = node.num_cells;
                info!(
                    "{}- leaf @page_num={} (num_cells: {})",
                    Self::indent(indent_level),
                    page_num,
                    num_cells
                );

                for i in 0..num_cells {
                    let cell_key = node.get_cell_key(i);
                    info!("{}- {}", Self::indent(indent_level), cell_key);

                    // let cell_value = node.get_cell_value(i);
                    // let mut row_data = Row {
                    //     id: 123,
                    //     email: String::from("123"),
                    //     username: String::from("!@3"),
                    // };
                    // deserialize_row(cell_value, &mut row_data).unwrap();
                    // info!(
                    //     "{}- id  {}, username: {}, email: {}",
                    //     Self::indent(indent_level),
                    //     row_data.id,
                    //     row_data.username,
                    //     row_data.email
                    // );
                }
            }
            NodeType::Internal => {
                let node = self.get_page_internal(page_num).unwrap();

                let num_keys = node.num_keys;
                info!(
                    "{}- internal @page_num={} (num_childs: {})",
                    Self::indent(indent_level),
                    page_num,
                    num_keys + 1
                );

                let mut child_nums: Vec<(i32, u32)> = vec![];
                for i in 0..num_keys {
                    let elem = node.cells[i as usize];
                    child_nums.push((elem.0 as i32, elem.1));
                }
                child_nums.push((-1, node.right_child));

                let mut index = 0;
                for child in child_nums {
                    let key: i32 = child.0 as i32;
                    let num = child.1;
                    info!("Index: {} || key < {}", index, key);
                    self.print_b_tree(num as usize, indent_level + 1);
                    index = index + 1;
                }
            }
        }
    }
}

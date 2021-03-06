//! Causally sort a collection of messages.
//!
//! Returns messages sorted from newest to oldest.
//!
//! If `message b` includes a reference to `message a` then we say that `message b` _must_ have been
//! published after `message a`, assuming these assumptions hold:
//! - The hash function is not broken (Two different sets of bytes return the same hash.)
//! - The person publishing `message b` has not guessed a valid hash of a message before it was
//! published (extremely unlikely.)
//! - The person publishing `message b` is not a time traveller. 
//!
//! This function uses [daggy]() to build a [dag]() of references between messages and then
//! topologically sorts them.
//!
//! If a message is not referenced by any messages you can expect it to be sorted to the start of
//! the results (it is so new no one has referenced it yet). 
//!
use daggy::{Dag, NodeIndex, Walker};
use petgraph::visit::Topo;
use serde_json::Value;
use ssb_multiformats::multihash::Multihash;
use std::collections::HashMap;

pub fn causal_sort<T: AsRef<str>, K: Copy>(msgs: &[(Multihash, K, T)]) -> Vec<K> {
    // Thought: Can we enumerate the iter and use the index as a key for one or both of the hashes?
    let (dag, _, node_to_key_id) = msgs
        .iter()
        .map(|(key, key_id, msg)| {
            let value: Value = serde_json::from_str(msg.as_ref()).unwrap_or(Value::Null);
            let mut refs = Vec::new();
            // Recursively search through the object searching for Multihashes
            find_all_links(&value, &mut refs);
            (key, key_id, refs)
        })
        .fold(
            (
                Dag::<u32, u32, usize>::new(),
                HashMap::<Multihash, NodeIndex<usize>>::new(),
                HashMap::<NodeIndex<usize>, K>::new(),
            ),
            |(mut dag, mut hash_to_node, mut node_to_key_id), (key, key_id, refs)| {
                // Check if we've already created a node for key
                let key_node = hash_to_node
                    .entry(key.clone())
                    .or_insert_with(|| dag.add_node(1))
                    .clone();
                node_to_key_id.entry(key_node).or_insert(*key_id);

                refs.iter().for_each(|reference| {
                    let ref_node = hash_to_node
                        .entry(reference.clone())
                        .or_insert_with(|| dag.add_node(1));
                    dag.add_edge(key_node.clone(), *ref_node, 1).expect("The dag has a cycle. This is _VERY_ unexpected. Either the SHA256 hash function is broken, someone is a time traveller, or someone guessed a hash of a message before it was ever created. Most likely this module has a bug :)");
                });

                (dag, hash_to_node, node_to_key_id)
            },
        );

    // sort the dag
    let graph = dag.graph();
    let topo = Topo::new(graph);
    topo.iter(graph)
        // filter_map the sorted nodes into multihashes, taking only the ones that were for the
        // keys we passed in
        .filter_map(|node| node_to_key_id.get(&node))
        .map(|i| *i)
        .collect()
}

fn find_all_links(obj: &Value, keys: &mut Vec<Multihash>) {
    if let Some(st) = obj.as_str() {
        if let Ok((mh, _)) = Multihash::from_legacy(st.as_bytes()) {
            keys.push(mh)
        }
    }

    match obj {
        Value::Array(arr) => {
            for val in arr {
                find_all_links(val, keys);
            }
        }
        Value::Object(kv) => {
            for val in kv.values() {
                match val {
                    Value::Object(_) => find_all_links(val, keys),
                    Value::Array(_) => find_all_links(val, keys),
                    Value::String(_) => find_all_links(val, keys),
                    _ => (),
                }
            }
        }
        _ => (),
    }
}

#[cfg(test)]
mod tests {
    use crate::{causal_sort, find_all_links};
    use serde_json::{json, to_string};
    use ssb_multiformats::multihash::Multihash;

    #[test]
    fn it_works() {
        let k1 = Multihash::from_legacy(b"%rootBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256")
            .unwrap()
            .0;
        let v1_value = json!({
            "previous":  "%1AfrBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256",
            "nested": {
                "previous":  "%2AfrBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256",
                "arry": ["%3AfrBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256", "%4AfrBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256" ]
            }
        });
        let v1 = to_string(&v1_value).unwrap();

        let k2 = Multihash::from_legacy(b"%reply1K7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256")
            .unwrap()
            .0;
        let v2_value = json!({
            "root":  "%rootBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256",
        });
        let v2 = to_string(&v2_value).unwrap();
        let k3 = Multihash::from_legacy(b"%reply2K7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256")
            .unwrap()
            .0;
        let v3_value = json!({
            "root":  "%rootBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256",
            "previous": "%reply1K7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256"
        });
        let v3 = to_string(&v3_value).unwrap();

        let unsorted = [
            (k2.clone(), 2, v2),
            (k1.clone(), 1, v1),
            (k3.clone(), 3, v3),
        ];
        let sorted = causal_sort(&unsorted[..]);

        assert_eq!(sorted.as_slice(), [3,2,1])
    }
    #[test]
    fn it_works_with_orphaned_messages() {
        let k1 = Multihash::from_legacy(b"%rootBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256")
            .unwrap()
            .0;
        let v1_value = json!({
            "previous":  "%1AfrBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256",
            "nested": {
                "previous":  "%2AfrBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256",
                "arry": ["%3AfrBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256", "%4AfrBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256" ]
            }
        });
        let v1 = to_string(&v1_value).unwrap();

        let k2 = Multihash::from_legacy(b"%reply1K7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256")
            .unwrap()
            .0;
        let v2_value = json!({
            "root":  "%rootBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256",
        });
        let v2 = to_string(&v2_value).unwrap();
        let k3 = Multihash::from_legacy(b"%orphanK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256")
            .unwrap()
            .0;
        let v3_value = json!({
        });
        let v3 = to_string(&v3_value).unwrap();

        let unsorted = [
            (k2.clone(),2, v2),
            (k3.clone(),3, v3),
            (k1.clone(),1, v1),
        ];
        let sorted = causal_sort(&unsorted[..]);

        assert_eq!(sorted.as_slice(), [3,2,1])
    }

    #[test]
    fn find_all_links_works() {
        let value = json!({
            "previous":  "%1AfrBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256",
            "nested": {
                "previous":  "%2AfrBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256",
                "arry": ["%3AfrBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256", "%4AfrBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256" ]
            }
        });

        let mut keys = Vec::new();
        find_all_links(&value, &mut keys);
        assert_eq!(keys.len(), 4);
    }
}

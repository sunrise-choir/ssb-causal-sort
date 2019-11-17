use daggy::{Dag, NodeIndex, Walker};
use petgraph::visit::Topo;
use serde_json::Value;
use ssb_multiformats::multihash::Multihash;
use std::collections::HashMap;

// If a message references another then the referencer must come afterwards causally.
// TBD how to include "missing context"

pub fn causal_sort(msgs: &[(Multihash, &[u8])]) -> Vec<Multihash> {
    let (dag, _, node_to_hash) = msgs
        .iter()
        .map(|(key, msg)| {
            let value: Value = serde_json::from_slice(msg).unwrap();
            let mut refs = Vec::new();
            // get all the links in each message.
            find_all_links(&value, &mut refs);
            (key, refs)
        })
        .fold(
            (
                Dag::<u32, u32, usize>::new(),
                HashMap::<Multihash, NodeIndex<usize>>::new(),
                HashMap::<NodeIndex<usize>, Multihash>::new(),
            ),
            |(mut dag, mut hash_to_node, mut node_to_hash), (key, refs)| {
                // Check if we've already created a node for key
                let key_node = hash_to_node
                    .entry(key.clone())
                    .or_insert_with(|| dag.add_node(1))
                    .clone();
                node_to_hash.entry(key_node).or_insert(key.clone());

                refs.iter().for_each(|reference| {
                    let ref_node = hash_to_node
                        .entry(reference.clone())
                        .or_insert_with(|| dag.add_node(1));
                    node_to_hash.entry(*ref_node).or_insert(reference.clone());
                    dag.add_edge(key_node.clone(), *ref_node, 1).unwrap();
                });

                (dag, hash_to_node, node_to_hash)
            },
        );

    // sort the dag
    let graph = dag.graph();
    let topo = Topo::new(graph);
    topo.iter(graph)
        // map the sorted nodes into multihashes
        .map(|node| node_to_hash[&node].clone())
        // filter out all the references that are not ones we're interested in
        .filter(|key| {
            msgs.iter()
                .position(|(msg_key, _)| msg_key == key)
                .is_some()
        })
        .collect()
    // tie break by a timestamp
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
    use serde_json::{json, to_vec};
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
        let v1 = to_vec(&v1_value).unwrap();

        let k2 = Multihash::from_legacy(b"%reply1K7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256")
            .unwrap()
            .0;
        let v2_value = json!({
            "root":  "%rootBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256",
        });
        let v2 = to_vec(&v2_value).unwrap();
        let k3 = Multihash::from_legacy(b"%reply2K7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256")
            .unwrap()
            .0;
        let v3_value = json!({
            "root":  "%rootBOK7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256",
            "previous": "%reply1K7pZikWM6aupei3PuE5ghRtFM44nrsX0FuBWY=.sha256"
        });
        let v3 = to_vec(&v3_value).unwrap();

        let unsorted = [
            (k2.clone(), v2.as_slice()),
            (k1.clone(), v1.as_slice()),
            (k3.clone(), v3.as_slice()),
        ];
        let sorted = causal_sort(&unsorted[..]);

        assert_eq!(sorted.as_slice(), [k3,k2,k1])
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

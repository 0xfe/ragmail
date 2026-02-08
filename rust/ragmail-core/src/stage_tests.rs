use crate::Stage;

#[test]
fn stage_names_are_stable() {
    assert_eq!(Stage::Download.as_str(), "download");
    assert_eq!(Stage::Split.as_str(), "split");
    assert_eq!(Stage::Index.as_str(), "index");
    assert_eq!(Stage::Clean.as_str(), "clean");
    assert_eq!(Stage::Vectorize.as_str(), "vectorize");
    assert_eq!(Stage::Ingest.as_str(), "ingest");
}

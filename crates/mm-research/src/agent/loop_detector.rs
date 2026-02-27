use std::collections::HashSet;

/// Compute the Jaccard similarity between two strings based on word sets.
///
/// Returns a value between 0.0 (completely different) and 1.0 (identical).
/// Two empty strings are considered identical (returns 1.0).
pub fn jaccard_similarity(a: &str, b: &str) -> f64 {
    let set_a: HashSet<&str> = a.split_whitespace().collect();
    let set_b: HashSet<&str> = b.split_whitespace().collect();

    if set_a.is_empty() && set_b.is_empty() {
        return 1.0;
    }

    let intersection = set_a.intersection(&set_b).count();
    let union = set_a.union(&set_b).count();

    if union == 0 {
        return 1.0;
    }

    intersection as f64 / union as f64
}

/// Default similarity threshold above which consecutive tool calls
/// are considered a loop.
pub const LOOP_THRESHOLD: f64 = 0.85;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identical_strings() {
        let sim = jaccard_similarity("hello world foo", "hello world foo");
        assert!((sim - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_completely_different() {
        let sim = jaccard_similarity("hello world", "foo bar baz");
        assert!((sim - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_partial_overlap() {
        // {"hello", "world", "foo"} vs {"hello", "world", "bar"}
        // intersection = {"hello", "world"} = 2
        // union = {"hello", "world", "foo", "bar"} = 4
        // similarity = 2/4 = 0.5
        let sim = jaccard_similarity("hello world foo", "hello world bar");
        assert!((sim - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_empty_strings() {
        let sim = jaccard_similarity("", "");
        assert!((sim - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_one_empty() {
        let sim = jaccard_similarity("hello world", "");
        assert!((sim - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_subset() {
        // {"a", "b"} vs {"a", "b", "c"}
        // intersection = 2, union = 3 → 2/3
        let sim = jaccard_similarity("a b", "a b c");
        assert!((sim - 2.0 / 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_duplicate_words_ignored() {
        // Sets collapse duplicates: {"hello", "hello"} → {"hello"}
        let sim = jaccard_similarity("hello hello hello", "hello");
        assert!((sim - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_loop_detection_scenario() {
        // Simulate two similar tool call descriptions
        let call_1 = r#"get_fundamentals ticker AAPL period annual limit 5"#;
        let call_2 = r#"get_fundamentals ticker AAPL period annual limit 5"#;
        let sim = jaccard_similarity(call_1, call_2);
        assert!(sim > LOOP_THRESHOLD);

        // Different tools should not trigger
        let call_3 = r#"get_news ticker MSFT limit 10"#;
        let sim2 = jaccard_similarity(call_1, call_3);
        assert!(sim2 < LOOP_THRESHOLD);
    }
}

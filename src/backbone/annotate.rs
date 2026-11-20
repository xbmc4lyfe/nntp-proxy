//! Writing detection findings into the operator's config file as comments.
//!
//! Only comment lines carrying [`MARKER`] are ever added or removed, inside
//! the `[[servers]]` block of the server they describe; everything else in
//! the file is left byte for byte. A note whose wording has not changed is
//! left alone, so its date says when the finding first appeared.

/// Prefix of every comment line this module owns.
pub(crate) const MARKER: &str = "# backbone-check";
/// Longest comment line written, so notes stay readable in an editor.
const WIDTH: usize = 92;

/// The text with `server`'s notes replaced by `note` (dated `date`), or
/// `None` when the server is not in the file or nothing would change.
pub(crate) fn annotate(text: &str, server: &str, note: &str, date: &str) -> Option<String> {
    let note = &sanitize(note);
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let (start, end) = server_block(&lines, server)?;
    let existing: Vec<&str> = lines[start..end]
        .iter()
        .copied()
        .filter(|line| is_note(line))
        .collect();
    if comparable(&note_text(&existing)) == comparable(note) {
        return None;
    }

    let block: Vec<&str> = lines[start..end]
        .iter()
        .copied()
        .filter(|line| !is_note(line))
        .collect();
    let anchor = block
        .iter()
        .position(|line| key_of(line) == Some("backbone"))
        .or_else(|| {
            block
                .iter()
                .position(|line| key_of(line) == Some("name"))
                .map(|name| name + 1)
        })?;
    let newline = if text.contains("\r\n") { "\r\n" } else { "\n" };
    let indent: String = block
        .get(anchor)
        .or_else(|| block.get(anchor.saturating_sub(1)))
        .map(|line| {
            line.chars()
                .take_while(|c| *c == ' ' || *c == '\t')
                .collect()
        })
        .unwrap_or_default();
    let comment = wrap(note, date)
        .into_iter()
        .map(|line| format!("{indent}{line}{newline}"))
        .collect::<Vec<_>>();

    let mut out = String::with_capacity(text.len() + 256);
    lines[..start].iter().for_each(|line| out.push_str(line));
    block[..anchor].iter().for_each(|line| out.push_str(line));
    // A last line without a newline would otherwise run into the note.
    if !out.is_empty() && out.strip_suffix('\n').is_none() {
        out.push_str(newline);
    }
    comment.iter().for_each(|line| out.push_str(line));
    block[anchor..].iter().for_each(|line| out.push_str(line));
    lines[end..].iter().for_each(|line| out.push_str(line));
    (out != text).then_some(out)
}

/// Whether a config line is one of this module's own comments: the marker
/// followed by its date or a continuation colon, so an operator comment that
/// merely starts with the same word is left alone.
fn is_note(line: &str) -> bool {
    line.trim_start()
        .strip_prefix(MARKER)
        .is_some_and(|rest| rest.strip_prefix(" (").is_some() || rest.strip_prefix(':').is_some())
}

/// For each line, whether it begins inside a multi-line string, where
/// nothing is a header, key or note. Tracks TOML's four string kinds
/// (with escapes in the basic ones) and comments, so a `"""` inside a
/// one-line string or after a backslash does not open or close one.
fn inside_strings(lines: &[&str]) -> Vec<bool> {
    #[derive(Clone, Copy, PartialEq)]
    enum In {
        Code,
        MultiBasic,
        MultiLiteral,
    }
    let mut state = In::Code;
    lines
        .iter()
        .map(|line| {
            let starts_inside = state != In::Code;
            let chars: Vec<char> = line.chars().collect();
            let triple = |at: usize, quote: char| {
                chars
                    .get(at..at + 3)
                    .is_some_and(|run| run.iter().all(|c| *c == quote))
            };
            let mut at = 0;
            while at < chars.len() {
                match state {
                    In::MultiBasic => {
                        if chars[at] == '\\' {
                            at += 2;
                        } else if triple(at, '"') {
                            state = In::Code;
                            at += 3;
                        } else {
                            at += 1;
                        }
                    }
                    In::MultiLiteral => {
                        if triple(at, '\'') {
                            state = In::Code;
                            at += 3;
                        } else {
                            at += 1;
                        }
                    }
                    In::Code => match chars[at] {
                        '#' => break,
                        '"' if triple(at, '"') => {
                            state = In::MultiBasic;
                            at += 3;
                        }
                        '\'' if triple(at, '\'') => {
                            state = In::MultiLiteral;
                            at += 3;
                        }
                        '"' => {
                            // A one-line basic string: skip to its closing quote.
                            at += 1;
                            while at < chars.len() && chars[at] != '"' {
                                at += if chars[at] == '\\' { 2 } else { 1 };
                            }
                            at += 1;
                        }
                        '\'' => {
                            at += 1;
                            while at < chars.len() && chars[at] != '\'' {
                                at += 1;
                            }
                            at += 1;
                        }
                        _ => at += 1,
                    },
                }
            }
            starts_inside
        })
        .collect()
}

/// Line range of the `[[servers]]` block whose `name` is `server`.
fn server_block(lines: &[&str], server: &str) -> Option<(usize, usize)> {
    let inside = inside_strings(lines);
    let header = |index: usize| {
        !inside[index] && lines[index].trim_start_matches('\u{feff}').trim() == "[[servers]]"
    };
    let table_start = |index: usize| {
        !inside[index]
            && lines[index]
                .trim_start_matches('\u{feff}')
                .trim_start()
                .strip_prefix('[')
                .is_some()
    };
    let mut index = 0;
    while index < lines.len() {
        if header(index) {
            let start = index + 1;
            let end = (start..lines.len())
                .find(|&line| table_start(line))
                .unwrap_or(lines.len());
            let named = (start..end).any(|line| {
                !inside[line]
                    && key_of(lines[line]) == Some("name")
                    && string_value(lines[line]).as_deref() == Some(server)
            });
            if named {
                return Some((start, end));
            }
            index = end;
        } else {
            index += 1;
        }
    }
    None
}

/// The key of a `key = value` line, ignoring comments.
fn key_of(line: &str) -> Option<&str> {
    let line = line.trim_start();
    if line.strip_prefix('#').is_some() {
        return None;
    }
    let (key, _) = line.split_once('=')?;
    Some(key.trim())
}

/// The string value of a `key = "value"` or `key = 'value'` line, with the
/// common escapes of a basic string undone.
fn string_value(line: &str) -> Option<String> {
    let (_, value) = line.split_once('=')?;
    let value = value.trim_start();
    if let Some(literal) = value.strip_prefix('\'') {
        return literal.split_once('\'').map(|(value, _)| value.to_owned());
    }
    let mut chars = value.strip_prefix('"')?.chars();
    let mut out = String::new();
    while let Some(c) = chars.next() {
        match c {
            '"' => return Some(out),
            '\\' => match chars.next()? {
                'n' => out.push('\n'),
                't' => out.push('\t'),
                other => out.push(other),
            },
            c => out.push(c),
        }
    }
    None
}

/// The words of existing notes, without marker, date and line breaks.
fn note_text(lines: &[&str]) -> String {
    let words: Vec<&str> = lines
        .iter()
        .map(|line| {
            let rest = line.trim_start().trim_start_matches(MARKER).trim_start();
            // The first line carries "(YYYY-MM-DD):".
            let rest = rest
                .strip_prefix('(')
                .and_then(|r| r.split_once("):"))
                .map_or(rest, |(_, after)| after);
            rest.strip_prefix(':').unwrap_or(rest).trim()
        })
        .collect();
    normalize_spaces(&words.join(" "))
}

fn normalize_spaces(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Server reply text can carry control characters, which TOML forbids even
/// in comments; one would leave the file unloadable.
fn sanitize(note: &str) -> String {
    note.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
}

/// A note's wording without its check counts, so a count that moves by one
/// between checks does not rewrite the file every few hours.
fn comparable(note: &str) -> String {
    let words: Vec<&str> = note.split_whitespace().collect();
    let mut kept = Vec::with_capacity(words.len());
    let mut index = 0;
    while index < words.len() {
        let is_count = |word: &str| word.chars().all(|c| c.is_ascii_digit()) && !word.is_empty();
        if words[index] == "(met"
            && words.get(index + 2) == Some(&"of")
            && words.get(index + 1).is_some_and(|w| is_count(w))
            && words.get(index + 3).is_some_and(|w| is_count(w))
        {
            kept.push("(met");
            kept.push("of");
            index += 4;
        } else {
            kept.push(words[index]);
            index += 1;
        }
    }
    kept.join(" ")
}

/// `note` as marker comment lines: the first dated, the rest continuing.
fn wrap(note: &str, date: &str) -> Vec<String> {
    let first_prefix = format!("{MARKER} ({date}): ");
    let next_prefix = format!("{MARKER}: ");
    let mut lines = Vec::new();
    let mut current = first_prefix;
    let mut empty = true;
    for word in note.split_whitespace() {
        if !empty && current.chars().count() + 1 + word.chars().count() > WIDTH {
            lines.push(current.trim_end().to_owned());
            current = next_prefix.clone();
            empty = true;
        }
        if !empty {
            current.push(' ');
        }
        current.push_str(word);
        empty = false;
    }
    lines.push(current.trim_end().to_owned());
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: &str = r#"# top comment
[proxy]
port = 1119

[[servers]]
name = "alpha"
backbone = "omicron" # operator note
host = "news.alpha.example"

[[servers]]
name = "beta"
host = "news.beta.example"
max_connections = 10
"#;

    #[test]
    fn a_note_goes_above_the_backbone_line_and_nothing_else_moves() {
        let out = annotate(
            CONFIG,
            "alpha",
            "confirmed omicron: met 6 of 7 checks",
            "2026-11-16",
        )
        .unwrap();
        assert_eq!(
            out,
            CONFIG.replace(
                "backbone = \"omicron\"",
                "# backbone-check (2026-11-16): confirmed omicron: met 6 of 7 checks\nbackbone = \"omicron\""
            )
        );
    }

    #[test]
    fn without_a_backbone_line_the_note_goes_under_the_name() {
        let out = annotate(CONFIG, "beta", "detected omicron", "2026-11-16").unwrap();
        assert!(out.contains(
            "name = \"beta\"\n# backbone-check (2026-11-16): detected omicron\nhost = \"news.beta.example\""
        ));
        assert!(!out.contains("backbone = \"omicron\"\n# backbone-check"));
    }

    #[test]
    fn an_unchanged_note_is_left_with_its_first_date() {
        let once = annotate(
            CONFIG,
            "beta",
            "detected omicron: met 10 of 11",
            "2026-11-16",
        )
        .unwrap();
        assert_eq!(
            annotate(
                &once,
                "beta",
                "detected omicron:  met 10 of 11",
                "2026-11-20"
            ),
            None
        );
    }

    #[test]
    fn a_changed_note_replaces_the_old_one() {
        let once = annotate(CONFIG, "beta", "detected omicron", "2026-11-16").unwrap();
        let twice = annotate(&once, "beta", "detected abavia", "2026-11-20").unwrap();
        assert!(twice.contains("# backbone-check (2026-11-20): detected abavia\n"));
        assert!(!twice.contains("omicron\nhost"));
        assert_eq!(twice.matches(MARKER).count(), 1);
    }

    #[test]
    fn long_notes_wrap_and_read_back_as_one() {
        let note = "config says \"abavia\", but we think this server is on \"giganews\": its article numbering matches frugal and differs from xs, the other \"abavia\" server (7 of 8 checks)";
        let once = annotate(CONFIG, "alpha", note, "2026-11-16").unwrap();
        assert!(once.lines().all(|line| line.chars().count() <= WIDTH));
        assert!(once.matches(MARKER).count() > 1);
        assert_eq!(annotate(&once, "alpha", note, "2026-12-01"), None);
    }

    #[test]
    fn an_unknown_server_or_a_name_only_in_a_comment_is_not_touched() {
        assert_eq!(annotate(CONFIG, "gamma", "detected", "2026-11-16"), None);
        let commented = CONFIG.replace("name = \"beta\"", "# name = \"beta\"");
        assert_eq!(annotate(&commented, "beta", "detected", "2026-11-16"), None);
    }

    #[test]
    fn a_moved_check_count_alone_does_not_rewrite_the_note() {
        let once = annotate(
            CONFIG,
            "beta",
            "detected omicron (met 13 of 14 checks)",
            "2026-11-16",
        )
        .unwrap();
        assert_eq!(
            annotate(
                &once,
                "beta",
                "detected omicron (met 14 of 14 checks)",
                "2026-11-17"
            ),
            None
        );
        assert!(
            annotate(
                &once,
                "beta",
                "detected abavia (met 14 of 14 checks)",
                "2026-11-17"
            )
            .is_some()
        );
    }

    #[test]
    fn control_characters_from_a_server_never_reach_the_file() {
        let out = annotate(
            CONFIG,
            "beta",
            "could not check: 501 bad\u{1b}[0m\u{0}x",
            "2026-11-16",
        )
        .unwrap();
        assert!(!out.chars().any(|c| c.is_control() && c != '\n'));
        toml::from_str::<toml::Value>(&out).expect("the annotated file still parses");
    }

    #[test]
    fn triple_quotes_inside_a_one_line_string_open_nothing() {
        let config = format!("[proxy]\nnote = 'a\"\"\"b'\n{CONFIG}");
        let out = annotate(&config, "beta", "detected omicron", "2026-11-16").unwrap();
        assert!(out.contains("name = \"beta\"\n# backbone-check"));
    }

    #[test]
    fn an_escaped_quote_does_not_close_a_multi_line_string() {
        // A string holding `\"""` and a fake beta block, before the real file.
        let fake = "[extra]\nmotd = \"\"\"\nx \\\"\"\" y\n[[servers]]\nname = \"beta\"\n\"\"\"\n";
        let config = format!("{fake}{CONFIG}");
        let out = annotate(&config, "beta", "detected omicron", "2026-11-16").unwrap();
        assert_eq!(&out[..fake.len()], fake, "the string is untouched:\n{out}");
        assert!(out[fake.len()..].contains("name = \"beta\"\n# backbone-check"));
        toml::from_str::<toml::Value>(&out).expect("still parses");
    }

    #[test]
    fn crlf_files_stay_crlf() {
        let crlf = CONFIG.replace('\n', "\r\n");
        let out = annotate(&crlf, "beta", "detected omicron", "2026-11-16").unwrap();
        assert!(out.contains("# backbone-check (2026-11-16): detected omicron\r\n"));
        assert!(!out.replace("\r\n", "").contains('\n'));
    }
}

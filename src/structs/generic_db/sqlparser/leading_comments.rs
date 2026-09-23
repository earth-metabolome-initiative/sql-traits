//! Documentation read from the comment block written directly above a table or
//! a column.
//!
//! Each comment of a block stands alone on its lines, and no blank line splits
//! the block or separates it from what it documents.

use alloc::{string::String, vec::Vec};

use sqlparser::{
    ast::{
        CreateTable, Ident, Spanned,
        comments::{Comment, CommentWithSpan, Comments},
    },
    tokenizer::Location,
};

/// SQL text paired with the comments its parse found.
pub(super) struct CommentedSource<'text> {
    text: &'text str,
    line_starts: Vec<usize>,
    comments: Vec<CommentWithSpan>,
}

/// The documentation a `CREATE TABLE` statement writes for its table and
/// columns.
#[derive(Debug, Default)]
pub(super) struct TableDocumentation {
    pub(super) table: Option<String>,
    columns: Vec<(Ident, String)>,
}

impl TableDocumentation {
    /// Moves out the documentation written for the column `name`.
    pub(super) fn take_column(&mut self, name: &Ident) -> Option<String> {
        let position = self.columns.iter().position(|(column, _)| {
            column.value == name.value && column.quote_style.is_some() == name.quote_style.is_some()
        })?;
        Some(self.columns.swap_remove(position).1)
    }
}

impl<'text> CommentedSource<'text> {
    pub(super) fn new(text: &'text str, comments: Comments) -> Self {
        let comments = Vec::from(comments);
        let line_starts = if comments.is_empty() {
            Vec::new()
        } else {
            core::iter::once(0).chain(text.match_indices('\n').map(|(at, _)| at + 1)).collect()
        };
        Self { text, line_starts, comments }
    }

    pub(super) fn table_documentation(&self, create_table: &CreateTable) -> TableDocumentation {
        if self.comments.is_empty() {
            return TableDocumentation::default();
        }
        TableDocumentation {
            // The span starts at the name, after `CREATE ... TABLE [IF NOT EXISTS]`.
            table: self.leading(create_table.name.span().start, true),
            columns: create_table
                .columns
                .iter()
                .filter_map(|column| {
                    Some((column.name.clone(), self.leading(column.span().start, false)?))
                })
                .collect(),
        }
    }

    /// The comment block ending directly above `node`, where `keywords_between`
    /// lets words separate the block from `node`.
    fn leading(&self, node: Location, keywords_between: bool) -> Option<String> {
        let mut boundary = self.offset(node)?;
        let end = self.comments.partition_point(|comment| comment.span.start < node);
        let mut first = end;
        let mut words_allowed = keywords_between;
        while let Some(candidate) = first.checked_sub(1).map(|index| &self.comments[index]) {
            let start = self.offset(candidate.span.start)?;
            let finish = self.offset(candidate.span.end)?;
            if !adjacent(&self.text[finish..boundary], words_allowed) || !self.stands_alone(start) {
                break;
            }
            first -= 1;
            boundary = start;
            words_allowed = false;
        }

        let mut documentation = String::new();
        for line in self.comments[first..end].iter().flat_map(|comment| lines(&comment.comment)) {
            if documentation.is_empty() && line.is_empty() {
                continue;
            }
            if !documentation.is_empty() {
                documentation.push('\n');
            }
            documentation.push_str(line);
        }
        documentation.truncate(documentation.trim_end().len());
        (!documentation.is_empty()).then_some(documentation)
    }

    /// Whether only whitespace precedes `offset` on its line.
    fn stands_alone(&self, offset: usize) -> bool {
        let line_start = self.text[..offset].rfind('\n').map_or(0, |newline| newline + 1);
        self.text[line_start..offset].trim().is_empty()
    }

    /// The byte offset of a location the tokenizer counted in characters.
    fn offset(&self, location: Location) -> Option<usize> {
        let line = usize::try_from(location.line).ok()?.checked_sub(1)?;
        let column = usize::try_from(location.column).ok()?.checked_sub(1)?;
        let start = *self.line_starts.get(line)?;
        self.text[start..]
            .char_indices()
            .map(|(at, _)| start + at)
            .chain(core::iter::once(self.text.len()))
            .nth(column)
    }
}

/// Whether `gap` holds no blank line and nothing but whitespace, or words when
/// `words_allowed`.
fn adjacent(gap: &str, words_allowed: bool) -> bool {
    let mut segments = gap.split('\n');
    segments.next();
    segments.next_back();
    segments.all(|segment| !segment.trim().is_empty())
        && gap.chars().all(|character| {
            character.is_whitespace()
                || (words_allowed && (character.is_alphanumeric() || character == '_'))
        })
}

fn lines(comment: &Comment) -> impl Iterator<Item = &str> {
    comment.as_str().lines().map(str::trim)
}

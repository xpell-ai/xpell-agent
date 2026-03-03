type HeadingMatch = {
  level: number;
  title: string;
  normalized: string;
  line_index: number;
};

function parse_heading(line: string, line_index: number): HeadingMatch | undefined {
  const match = line.match(/^(#{1,6})\s+(.+?)\s*$/);
  if (!match) return undefined;
  const level = match[1].length;
  const title = match[2].trim();
  const normalized = normalize_heading_query(title);
  if (!normalized) return undefined;
  return {
    level,
    title,
    normalized,
    line_index
  };
}

export function normalize_heading_query(input: string): string {
  return input.replace(/^#+\s*/u, "").trim().replace(/\s+/gu, " ").toLowerCase();
}

export function extract_section(
  md: string,
  heading_query: string
): { found: boolean; title?: string; content?: string; error?: string } {
  const normalized_query = normalize_heading_query(heading_query);
  if (!normalized_query) {
    return { found: false, error: "section_not_found: empty_query" };
  }
  const lines = md.split(/\r?\n/g);
  let start_heading: HeadingMatch | undefined;
  let end_index = lines.length;

  for (let idx = 0; idx < lines.length; idx += 1) {
    const heading = parse_heading(lines[idx], idx);
    if (!heading) continue;
    if (!start_heading && heading.normalized === normalized_query) {
      start_heading = heading;
      continue;
    }
    if (start_heading && heading.level <= start_heading.level) {
      end_index = idx;
      break;
    }
  }

  if (!start_heading) {
    return { found: false, error: `section_not_found:${normalized_query}` };
  }

  return {
    found: true,
    title: start_heading.title,
    content: lines.slice(start_heading.line_index, end_index).join("\n").trimEnd()
  };
}

export function list_top_headings(md: string): string[] {
  const lines = md.split(/\r?\n/g);
  const out: string[] = [];
  for (let idx = 0; idx < lines.length; idx += 1) {
    const heading = parse_heading(lines[idx], idx);
    if (!heading) continue;
    if (heading.level !== 2 && heading.level !== 3) continue;
    out.push(heading.title);
  }
  return out;
}

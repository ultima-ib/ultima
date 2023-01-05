export function takeLines(text: string, start: number, end: number) {
    const lines = text.split(/\r\n|\r|\n/);
    const split = lines.slice(start, end)
    return split.join('\n')
}

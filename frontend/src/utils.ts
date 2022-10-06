export const fancyZip = <T>(rows: T[][]): T[][] => rows[0].map((_, c) => rows.map(row => row[c]))

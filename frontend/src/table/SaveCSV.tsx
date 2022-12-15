import { TableData } from "./index"
import DownloadIcon from "@mui/icons-material/Download"
import { Button } from "@mui/material"

function buildCSV(headers: string[], rows: (string | number | null)[][]) {
    const csvHeaders = headers.join(",")
    const csvRows = rows
        .map((cells) => cells.map((it) => it?.toString() ?? "").join(","))
        .join("\r\n")
        return `data:text/csv;charset=utf-8,${csvHeaders}\r\n${csvRows}`
}

const SaveCSV = ({ headers, rows }: TableData) => {
    const saveCsv = () => {
        const csvContent = buildCSV(headers, rows)
        const encodedUri = encodeURI(csvContent)
        window.open(encodedUri)
    }

    return (
        <Button
            variant="contained"
            endIcon={<DownloadIcon />}
            onClick={saveCsv}
        >
            Save as CSV
        </Button>
    )
}

export default SaveCSV

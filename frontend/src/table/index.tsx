import { GenerateTableDataRequest } from "../api/types"
import { useTableData } from "../api/hooks"
import {
    Paper,
    TableContainer,
    Table,
    TableHead,
    TableBody,
    TableRow,
    TableCell,
    TableFooter,
    Button,
} from "@mui/material"
import DownloadIcon from "@mui/icons-material/Download"
import { fancyZip } from "../utils"
import { forwardRef } from "react"

const formatValue = (value: string | null | number) => {
    if (typeof value === "string") {
        return value
    } else if (value === null) {
        return ""
    } else {
        return value.toFixed(2)
    }
}

interface DataTableProps {
    input: GenerateTableDataRequest
    unique: string
}

const DataTable = forwardRef<HTMLTableSectionElement, DataTableProps>(
    (props, ref) => {
        const { data, error } = useTableData(props.input)
        if (error || !data) {
            return <>{error}</>
        }
        const headers = data.columns.map((it) => it.name)
        const zipped = fancyZip(data.columns.map((col) => col.values))

        const saveCsv = () => {
            const csvHeaders = headers.join(",")
            const rows = zipped
                .map((cells) =>
                    cells.map((it) => it?.toString() ?? "").join(","),
                )
                .join("\r\n")
            const csvContent = `data:text/csv;charset=utf-8,${csvHeaders}\r\n${rows}`
            const encodedUri = encodeURI(csvContent)
            window.open(encodedUri)
        }

        return (
            <Paper sx={{ overflow: "hidden", width: "100%" }}>
                <TableContainer sx={{ maxHeight: "calc(100vh - 100px)" }}>
                    <Table stickyHeader>
                        <TableHead ref={ref}>
                            <TableRow>
                                {headers.map((it) => (
                                    <TableCell key={props.unique + it}>
                                        {it}
                                    </TableCell>
                                ))}
                            </TableRow>
                        </TableHead>
                        <TableBody>
                            {zipped.map((values, index) => (
                                <TableRow
                                    key={`${props.unique}${index.toString()}`}
                                    hover
                                >
                                    {values.map((it, innerIndex) => (
                                        <TableCell
                                            key={`${props.unique}${
                                                headers[innerIndex]
                                            }${index}${it?.toString() ?? ""}`}
                                        >
                                            {formatValue(it)}
                                        </TableCell>
                                    ))}
                                </TableRow>
                            ))}
                        </TableBody>
                        <TableFooter>
                            <TableRow>
                                <TableCell colSpan={headers.length}>
                                    Total Rows: {zipped.length}
                                </TableCell>
                            </TableRow>
                            <TableRow>
                                <TableCell colSpan={headers.length}>
                                    <Button
                                        variant="contained"
                                        endIcon={<DownloadIcon />}
                                        onClick={saveCsv}
                                    >
                                        Save as CSV
                                    </Button>
                                </TableCell>
                            </TableRow>
                        </TableFooter>
                    </Table>
                </TableContainer>
            </Paper>
        )
    },
)

DataTable.displayName = "DataTable"

export default DataTable

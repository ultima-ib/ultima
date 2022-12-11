import { GenerateTableDataRequest, GenerateTableDataResponse } from "../api/types"
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
    Button, TableCellProps,
} from "@mui/material"
import { styled } from "@mui/material/styles"
import DownloadIcon from "@mui/icons-material/Download"
import { fancyZip } from "../utils"
import { forwardRef, useState } from "react"
import SummarizeIcon from "@mui/icons-material/Summarize"
import { SummaryStats } from "./SummaryStats"

const StickyTableCell = styled(TableCell)<TableCellProps>(({ theme }) => ({
    left: 0,
    position: "sticky",
    zIndex: theme.zIndex.appBar + 1,
    background: theme.palette.background.default,
}))

const formatValue = (value: string | null | number) => {
    if (typeof value === "string") {
        return value
    } else if (value === null) {
        return ""
    } else {
        return value.toFixed(2)
    }
}

interface DataTableBodyProps {
    data: GenerateTableDataResponse["columns"]
    unique: string
    showFooter: boolean
    stickyColIndex?: number
}

export const DataTableBody = forwardRef<HTMLTableSectionElement, DataTableBodyProps>(
    ({ data, unique, showFooter, stickyColIndex}, ref) => {
        const [dialogOpen, setDialogOpen] = useState(false)

        const headers = data.map((it) => it.name)
        const zipped = fancyZip(data.map((col) => col.values))

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

        const summarizeTable = () => {
            setDialogOpen(true)
        }

        return (
            <>
                <TableHead ref={ref}>
                    <TableRow>
                        {headers.map((it, index) => {
                            const Cell = index === stickyColIndex ? StickyTableCell : TableCell
                            return (
                                <Cell key={unique + it} sx={(theme) => ({ zIndex: theme.zIndex.appBar + 2})}>
                                    {it}
                                </Cell>
                            )
                        })}
                    </TableRow>
                </TableHead>
                <TableBody>
                    {zipped.map((values, index) => (
                        <TableRow
                            key={`${unique}${index.toString()}`}
                            hover
                        >
                            {values.map((it, innerIndex) => {
                                const Cell = innerIndex === stickyColIndex ? StickyTableCell : TableCell
                                const key = `${unique}${headers[innerIndex]}${index}${it?.toString() ?? ""}`
                                return (
                                    <Cell key={key}>
                                        {formatValue(it)}
                                    </Cell>
                                )
                            })}
                        </TableRow>
                    ))}
                </TableBody>
                {showFooter && (<TableFooter>
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
                    <TableRow>
                        <TableCell colSpan={headers.length}>
                            <Button
                                variant="contained"
                                endIcon={<SummarizeIcon />}
                                onClick={summarizeTable}
                            >
                                Summarize
                            </Button>
                        </TableCell>
                    </TableRow>
                </TableFooter>)}
                <SummaryStats table={{ columns: data }} openState={[dialogOpen, setDialogOpen]} />
            </>
        )
    },
)

DataTableBody.displayName = "DataTableBody"


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
        return (
            <Paper sx={{ overflow: "hidden", width: "100%" }}>
                <TableContainer sx={{ maxHeight: "calc(100vh - 100px)" }}>
                    <Table stickyHeader>
                        <DataTableBody
                            data={data.columns}
                            unique={props.unique}
                            ref={ref}
                            showFooter={true} />
                    </Table>
                </TableContainer>
            </Paper>
        )
    },
)

DataTable.displayName = "DataTable"

export default DataTable

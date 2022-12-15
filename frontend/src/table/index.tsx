import {
    GenerateTableDataRequest,
    GenerateTableDataResponse,
} from "../api/types"
import { useTableData } from "../api/hooks"
import {
    Paper,
    TableContainer,
    Table,
    TableHead,
    TableBody,
    TableRow,
    TableCell,
    Button,
    TableCellProps,
    Box, TableProps,
} from "@mui/material"
import { styled } from "@mui/material/styles"
import { fancyZip } from "../utils"
import { forwardRef, useState } from "react"
import SummarizeIcon from "@mui/icons-material/Summarize"
import { SummaryStats } from "./SummaryStats"
import SaveCSV from "./SaveCSV"

const StickyTableCell = styled(TableCell)<TableCellProps>(({ theme }) => ({
    left: 0,
    position: "sticky",
    zIndex: theme.zIndex.appBar + 1,
    background: theme.palette.background.default,
}))

const fmt = new Intl.NumberFormat('en', { maximumFractionDigits: 2, minimumFractionDigits: 2 })

const formatValue = (value: string | null | number) => {
    if (typeof value === "string") {
        return value
    } else if (value === null) {
        return ""
    } else {
        return fmt.format(value)
    }
}

export type TableData = {
    headers: string[]
    rows: (string | number | null)[][]
}

interface DataTableBodyProps extends TableProps {
    data: TableData
    unique: string
    stickyColIndex?: number
    hideSummarize?: boolean;
}

export const DataTableBody = forwardRef<
    HTMLTableSectionElement,
    DataTableBodyProps
>(({ data, unique, stickyColIndex, hideSummarize, ...props }, ref) => {

    const { headers, rows: zipped } = data

    return (
        <>
            <Table {...props}>
                <TableHead ref={ref}>
                    <TableRow>
                        {headers.map((it, index) => {
                            const Cell =
                                index === stickyColIndex
                                    ? StickyTableCell
                                    : TableCell
                            return (
                                <Cell
                                    key={unique + it}
                                    sx={(theme) => ({
                                        zIndex: theme.zIndex.appBar + 2,
                                    })}
                                >
                                    {it}
                                </Cell>
                            )
                        })}
                    </TableRow>
                </TableHead>
                <TableBody>
                    {zipped.map((values, index) => (
                        <TableRow key={`${unique}${index.toString()}`} hover>
                            {values.map((it, innerIndex) => {
                                const Cell =
                                    innerIndex === stickyColIndex
                                        ? StickyTableCell
                                        : TableCell
                                const key = `${unique}${
                                    headers[innerIndex]
                                }${index}${it?.toString() ?? ""}`
                                return (
                                    <Cell
                                        key={key}
                                        sx={{
                                            ":hover": {
                                                fontWeight: "bold",
                                            },
                                        }}
                                    >
                                        {formatValue(it)}
                                    </Cell>
                                )
                            })}
                        </TableRow>
                    ))}
                </TableBody>
            </Table>

        </>
    )
})

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
        const headers = data.columns.map((it) => it.name)
        const zipped = fancyZip(data.columns.map((col) => col.values))
        const [dialogOpen, setDialogOpen] = useState(false)


        const summarizeTable = () => {
            setDialogOpen(true)
        }

        return (
            <Paper sx={{ overflow: "hidden", width: "100%" }}>
                <TableContainer sx={{ maxHeight: "calc(100vh - 100px)" }}>
                    <DataTableBody
                        data={{ headers, rows: zipped }}
                        unique={props.unique}
                        ref={ref}
                        stickyHeader={true}
                    />
                </TableContainer>
                <Box
                    sx={{
                        display: "flex",
                        gap: 2,
                        pt: 4,
                        pb: 2,
                    }}
                >
                    <SaveCSV rows={zipped} headers={headers} />
                    <Button
                        variant="contained"
                        endIcon={<SummarizeIcon />}
                        onClick={summarizeTable}
                    >
                        Summarize
                    </Button>
                </Box>

                <SummaryStats
                    table={{ columns: data.columns }}
                    openState={[dialogOpen, setDialogOpen]}
                />
            </Paper>
        )
    },
)

DataTable.displayName = "DataTable"

export default DataTable

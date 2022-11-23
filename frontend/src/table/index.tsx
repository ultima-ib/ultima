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
} from "@mui/material"
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
                                <TableCell>
                                    Total Rows: {zipped.length}
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

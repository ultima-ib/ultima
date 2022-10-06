import {GenerateTableDataRequest} from "../api/types";
import {useTableData} from "../api/hooks";
import {Paper, TableContainer, Table, TableHead, TableBody, TableRow, TableCell} from "@mui/material";
import {fancyZip} from "../utils";

interface DataTableProps {
    input: GenerateTableDataRequest,
}

const DataTable = (props: DataTableProps) => {
    const data = useTableData(props.input)
    const headers = data.columns.map(it => it.name)
    const zipped = fancyZip(data.columns.map(col => col.values))

    return (
        <Paper sx={{overflow: 'hidden', width: '100%',}}>
            <TableContainer sx={{ maxHeight: 'calc(100vh - 100px)' }}>
                <Table stickyHeader>
                    <TableHead>
                        <TableRow>
                            {headers.map(it => <TableCell>{it}</TableCell>)}
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {zipped.map((values) => (
                            <TableRow>
                                {values.map(it => <TableCell>{(it ?? '').toString()}</TableCell>)}
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            </TableContainer>
        </Paper>
    )
}

export default DataTable

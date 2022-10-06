import {GenerateTableDataRequest} from "../api/types";
import {useTableData} from "../api/hooks";
import {Box} from "@mui/material";

interface DataTableProps {
    input: GenerateTableDataRequest,
}


const DataTable = (props: DataTableProps) => {
    const data = useTableData(props.input)
    const columns = data.columns

    return (
        <Box sx={{overflow: 'scroll'}}>
            <h3>Request</h3>
            <pre>
                {JSON.stringify(props.input, null, 4)}
            </pre>
            <h3>Response</h3>
            <pre>
                {JSON.stringify(columns, null, 4)}
            </pre>
        </Box>
    )
}

export default DataTable

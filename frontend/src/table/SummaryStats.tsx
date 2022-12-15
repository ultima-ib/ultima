import {
    Dialog,
    DialogContent,
    DialogTitle,
    CircularProgress,
} from "@mui/material"
import { Suspense, SetStateAction, Dispatch } from "react"
import { GenerateTableDataResponse } from "../api/types"
import { useDescribeTableData } from "../api/hooks"
import { DataTableBody } from "./index"
import { fancyZip } from "../utils"
import SaveCSV from "./SaveCSV"

const SummaryTable = (props: { table: GenerateTableDataResponse }) => {
    const data = useDescribeTableData(props.table)

    const headers = data.columns.map((it) => it.name)
    const zipped = fancyZip(data.columns.map((col) => col.values))

    return (
        <>
            <DataTableBody
                data={{ headers, rows: zipped }}
                unique={"summary"}
                stickyColIndex={data.columns.findIndex(
                    (it) => it.name === "describe",
                )}
                stickyHeader={false}
                hideSummarize
            />
            <SaveCSV headers={headers} rows={zipped} />
        </>
    )
}

export function SummaryStats(props: {
    table: GenerateTableDataResponse
    openState: [boolean, Dispatch<SetStateAction<boolean>>]
}) {
    const [open, setOpen] = props.openState

    const handleClose = () => {
        setOpen(false)
    }

    return (
        <div>
            <Dialog
                open={open}
                fullWidth
                maxWidth="xl"
                onClose={handleClose}
                scroll="paper"
                aria-labelledby="scroll-dialog-title"
            >
                <DialogTitle id="scroll-dialog-title">Summary</DialogTitle>
                <DialogContent dividers>
                    <Suspense fallback={<CircularProgress />}>
                        <SummaryTable table={props.table} />
                    </Suspense>
                </DialogContent>
            </Dialog>
        </div>
    )
}

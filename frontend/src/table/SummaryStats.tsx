import {
    Dialog,
    DialogContent,
    DialogTitle,
    CircularProgress, Table,
} from "@mui/material"
import { Suspense, SetStateAction, Dispatch } from "react"
import { GenerateTableDataResponse } from "../api/types"
import { useDescribeTableData } from "../api/hooks"
import { DataTableBody } from "./index"

export function SummaryStats(props: {
    table: GenerateTableDataResponse
    openState: [boolean, Dispatch<SetStateAction<boolean>>]
}) {
    const [open, setOpen] = props.openState
    const data = useDescribeTableData(props.table)

    const handleClose = () => {
        setOpen(false)
    }

    return (
        <div>
            <Dialog
                open={open}
                fullWidth
                maxWidth='sm'
                onClose={handleClose}
                scroll="paper"
                aria-labelledby="scroll-dialog-title"
            >
                <DialogTitle id="scroll-dialog-title">Summary</DialogTitle>
                <DialogContent dividers>
                    <Suspense fallback={<CircularProgress />}>
                        <Table>
                            <DataTableBody
                                data={data.columns}
                                unique={"summary"}
                                showFooter={false}
                                hover={true}
                            />
                        </Table>
                    </Suspense>
                </DialogContent>
            </Dialog>
        </div>
    )
}

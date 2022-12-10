import {
    Box,
    SpeedDial,
    SpeedDialIcon,
    SpeedDialAction,
    BoxProps,
    Fade,
    CircularProgress,
    Stack,
    IconButton, Paper,
} from "@mui/material"
import { styled } from "@mui/material/styles"
import { Suspense } from "react"
import SummarizeIcon from "@mui/icons-material/Summarize"

import CompareIcon from "@mui/icons-material/Compare"
import ArrowDownIcon from "@mui/icons-material/KeyboardArrowDown"
import { useState } from "react"
import { GenerateTableDataRequest } from "../api/types"
import { useDescribeTableData } from "../api/hooks"

export const SummarizeFab = (props: { summarize: (table: "primary" | "compare") => void }) => {
    return (
        <SpeedDial
            ariaLabel="Summarize"
            sx={{ position: "absolute", bottom: 16, right: 16 }}
            icon={<SpeedDialIcon icon={<SummarizeIcon />} />}
        >
            <SpeedDialAction
                icon={<CompareIcon />}
                onClick={() => props.summarize("compare")}
                tooltipTitle={"Summarize Compare Table"}
            />

            <SpeedDialAction
                icon={<CompareIcon />}
                onClick={() => props.summarize("primary")}
                tooltipTitle={"Summarize Primary Table"}
            />
        </SpeedDial>
    )
}

const SummaryContainer = styled(Box)<BoxProps>(({ theme }) => ({
    position: "absolute",
    bottom: theme.spacing(3),
    right: theme.spacing(3),
}))

const SummaryTable = (props: { table: GenerateTableDataRequest, hide: () => void }) => {
    const data = useDescribeTableData(props.table)
    console.log('data', data);

    return <Stack sx={{ height: 300, width: 300 }} component={Paper}>
        <Box sx={{display: 'flex'}}>
            <IconButton onClick={props.hide} sx={{ml: 'auto'}}>
                <ArrowDownIcon />
            </IconButton>
        </Box>
        <Box>{JSON.stringify(props.table)}</Box>
    </Stack>
}

const SummaryStats = (props: {
    primaryTable: GenerateTableDataRequest | undefined,
    compareTable: GenerateTableDataRequest | undefined,
}) => {
    const [tableName, setTableName] = useState<"primary" | "compare" | null>(null)
    const summarize = (table: "primary" | "compare") => {
        setTableName(table)
    }

    let table = tableName === "primary" ? props.primaryTable : props.compareTable

    const showing = tableName !== null && table !== undefined
    return (
        <SummaryContainer>
            <Fade in={!showing}>
                <div>
                    <SummarizeFab summarize={summarize} />
                </div>
            </Fade>
            <Fade in={showing}>
                <div>
                    <Suspense fallback={<CircularProgress />}>
                        {table && <SummaryTable table={table} hide={() => setTableName(null)} />}
                    </Suspense>
                </div>
            </Fade>
        </SummaryContainer>
    )
}

export default SummaryStats

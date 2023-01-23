import Aside from "./aside"
import { useReducer, useRef, useState, Suspense, useEffect } from "react"
import { useFRTB } from "./api/hooks"
import {
    buildRequest,
    InputStateContext,
    InputStateContextProvider,
    inputStateReducer,
} from "./aside/InputStateContext"
import { Box } from "@mui/material"
import TopBar from "./AppBar"
import DataTable from "./table"
import { GenerateTableDataRequest } from "./api/types"

export const AppWrapper = () => {
    const frtb = useFRTB()

    const init: InputStateContext = {
        dataSet: {
            fields: frtb.fields,
            measures: frtb.measures.map((it) => it.measure),
            groupby: [],
            measuresSelected: [],
            calcParams: frtb.calcParams,
        },
        canMeasureBeAggregated: (measure: string) => {
            const m = frtb.measures.find((it) => it.measure === measure)
            return m !== undefined && m.agg === null
        },
        overrides: {},
        filters: {},
        aggData: {},
        hideZeros: false,
        totals: false,
        calcParams: {},
        additionalRows: {
            rows: [],
            prepare: false,
        },
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        dispatcher: () => {},
    }

    const [context, dispatcher] = useReducer(inputStateReducer, init)

    const [buildTableReq, setBuildTableReq] = useState<
        GenerateTableDataRequest | undefined
    >(undefined)
    const [buildComparisonTableReq, setBuildComparisonTableReq] = useState<
        GenerateTableDataRequest | undefined
    >(undefined)

    const mainDataTableHeadRef = useRef<HTMLTableSectionElement | null>(null)
    const compareDataTableRef = useRef<HTMLTableSectionElement | null>(null)

    const run = (setter: typeof setBuildTableReq) => () => {
        setter(buildRequest(context, frtb))
    }

    useEffect(() => {
        requestIdleCallback(() => {
            if (mainDataTableHeadRef.current && compareDataTableRef.current) {
                const mainHead = mainDataTableHeadRef.current
                const compareHead = compareDataTableRef.current
                if (mainHead.scrollHeight > compareHead.scrollHeight) {
                    compareHead.style.height = `${mainHead.scrollHeight}px`
                } else {
                    mainHead.style.height = `${compareHead.scrollHeight}px`
                }
            }
        })
    })

    return (
        <Box sx={{ display: "flex", height: "100%" }}>
            <InputStateContextProvider
                value={{
                    ...context,
                    dispatcher,
                }}
            >
                <Aside />
                <TopBar
                    onRunClick={run(setBuildTableReq)}
                    onCompareClick={
                        buildComparisonTableReq
                            ? () => setBuildComparisonTableReq(undefined)
                            : run(setBuildComparisonTableReq)
                    }
                    compareButtonLabel={
                        buildComparisonTableReq ? "Stop Comparing" : "Compare"
                    }
                >
                    <Suspense fallback="Loading...">
                        <Box
                            sx={{
                                display: "flex",
                                gap: 2,
                            }}
                        >
                            {buildTableReq && (
                                <DataTable
                                    unique="main"
                                    ref={mainDataTableHeadRef}
                                    input={buildTableReq}
                                />
                            )}
                            {buildComparisonTableReq && (
                                <DataTable
                                    unique="comp"
                                    ref={compareDataTableRef}
                                    input={buildComparisonTableReq}
                                />
                            )}
                        </Box>
                    </Suspense>
                </TopBar>
            </InputStateContextProvider>
        </Box>
    )
}

export default AppWrapper

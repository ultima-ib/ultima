import Aside from "./aside"
import { useReducer, useRef, useState, Suspense, useEffect } from "react"
import { useFRTB } from "./api/hooks"
import {
    InputStateContext,
    InputStateContextProvider,
    inputStateReducer,
} from "./aside/InputStateContext"
import { Box } from "@mui/material"
import TopBar from "./AppBar"
import DataTable from "./table"
import { GenerateTableDataRequest } from "./api/types"
import { mapFilters } from "./utils"

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
        const data = context.dataSet
        const measures = data.measuresSelected.map(
            (measure: string): [string, string] => {
                const m = frtb.measures.find((it) => it.measure === measure)!
                const agg: string = context.aggData[m.measure]
                // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
                return [m.measure, agg ?? m.agg]
            },
        )
        const obj: GenerateTableDataRequest = {
            filters: mapFilters(context.filters),
            groupby: data.groupby,
            measures,
            overrides: Object.values(context.overrides),
            hide_zeros: context.hideZeros,
            totals: context.totals,
            calc_params: context.calcParams,
        }
        setter(obj)
        console.log(JSON.stringify(obj, null, 2))
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

    const copyTable = (tableData: GenerateTableDataRequest | undefined) => {
        if (tableData === undefined) {
            return
        }
        const data = {
            ...tableData,
            name: "Shared Content",
        }
        navigator.clipboard
            .writeText(JSON.stringify(data, null, 2))
            .then(() => {
                // todo: show snackbar
            })
            .catch(() => {
                // todo: show snackbar
            })
    }

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
                    copyMainTable={() => copyTable(buildTableReq)}
                    copyComparisonTable={() =>
                        copyTable(buildComparisonTableReq)
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

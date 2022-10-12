import Aside from "./aside"
import { useReducer, useRef, useState, Suspense } from "react"
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

    const calcParams = useRef<Record<string, string>>({})
    const onCalcParamsChange = (name: string, value: string) => {
        calcParams.current[name] = value
    }

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
        calcParamsUpdater: onCalcParamsChange,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        dispatcher: () => {},
    }

    const [context, dispatcher] = useReducer(inputStateReducer, init)

    const [buildTableReq, setBuildTableReq] = useState<
        GenerateTableDataRequest | undefined
    >(undefined)

    const run = () => {
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
            calc_params: calcParams.current,
        }
        setBuildTableReq(obj)
        console.log(JSON.stringify(obj, null, 2))
    }

    return (
        <Box sx={{ display: "flex", height: "100%" }}>
            <InputStateContextProvider
                value={{
                    ...context,
                    dispatcher,
                }}
            >
                <Aside onCalcParamsChange={onCalcParamsChange} />
                <TopBar onRunClick={run}>
                    <Suspense fallback="Loading...">
                        {buildTableReq && <DataTable input={buildTableReq} />}
                    </Suspense>
                </TopBar>
            </InputStateContextProvider>
        </Box>
    )
}

export default AppWrapper

import Aside from './board';
import {useReducer, useRef, useState, Suspense} from "react";
import {Filter, Override} from "./types";
import {useFRTB} from "../api/hooks";
import {InputStateContextProvider, inputStateReducer} from "./InputStateContext";
import {Box} from "@mui/material";
import TopBar from "../AppBar";
import DataTable from "../table";
import {GenerateTableDataRequest} from "../api/types";


export const Initial = () => {
    const frtb = useFRTB();


    const init = {
        dataSet: {
            fields: frtb.fields,
            measures: frtb.measures.map(it => it.measure),
            groupby: [],
            overwrites: [],
            measuresSelected: [],
            calcParams: frtb.calcParams
        },
        canMeasureBeAggregated: (measure: string) => {
            const m = frtb.measures.find(it => it.measure === measure)
            return m !== undefined && m.agg === null
        },
        overrides: {},
        filters: {},
        aggData: {},
        hideZeros: false,
        totals: false,
    }
    // @ts-expect-error: i can't get the signature in line with the type declaration
    const [context, dispatcher] = useReducer(inputStateReducer, init);
    const calcParams = useRef<{[k: string]: string}>({});

    const [buildTableReq, setBuildTableReq] = useState<GenerateTableDataRequest | undefined>(undefined);

    const run = () => {
        const data = context.dataSet
        const measures = data.measuresSelected.map((measure: string) => {
            const m = frtb.measures.find(it => it.measure === measure)
            if (!m) return
            const agg: string = context.aggData[m.measure as any];
            return [m.measure, agg ?? m.agg]
        })
        const mapFilters = (f: object) => Object.values(f).map((it: any) => Object.values(it) as Filter[])
        const obj = {
            filters: mapFilters(context.filters),
            groupby: data.groupby,
            measures,
            overrides: Object.values(context.overrides) as Override[],
            hide_zeros: context.hideZeros,
            totals: context.totals,
            calc_params: calcParams.current
        }
        setBuildTableReq(obj)
        console.log(JSON.stringify(obj, null, 2))
    }


    const onCalcParamsChange = (name: string, value: string) => {
        calcParams.current[name] = value
    }

    return (
        <Box sx={{display: 'flex', height: '100%'}}>
            <InputStateContextProvider value={{
                ...context,
                dispatcher
            }}>
                <Aside onCalcParamsChange={onCalcParamsChange} />
                <TopBar onRunClick={run}>
                    <Suspense fallback="Loading...">
                        {buildTableReq && <DataTable input={buildTableReq}/>}
                    </Suspense>
                </TopBar>

            </InputStateContextProvider>
        </Box>
    )
}

export { Aside }

export default Initial

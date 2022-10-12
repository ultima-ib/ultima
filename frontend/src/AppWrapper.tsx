import Aside from "./aside"
import { useReducer, useRef, useState, Suspense } from "react"
import { useFRTB } from "./api/hooks"
import {
    InputStateContext,
    InputStateContextProvider,
    inputStateReducer,
} from "./aside/InputStateContext"
import { Box, Tab, Tabs, Grid, IconButton } from "@mui/material"
import TopBar from "./AppBar"
import DataTable from "./table"
import { GenerateTableDataRequest } from "./api/types"
import { mapFilters } from "./utils"
import { a11yProps, TabPanel, useTabs } from "./tabs"
import AddIcon from "@mui/icons-material/Add"

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

    const {
        activeTab: activeTabRight,
        handleActiveTabChange: handleActiveTabChangeRight,
    } = useTabs()
    const {
        activeTab: activeTabLeft,
        handleActiveTabChange: handleActiveTabChangeLeft,
    } = useTabs()

    const [buildTableReq, setBuildTableReq] = useState<
        Record<number, GenerateTableDataRequest | undefined>
    >({})

    const run = () => {
        const data = context.dataSet
        const measures = data.measuresSelected.map(
            (measure: string): [string, string] => {
                const m = frtb.measures.find((it) => it.measure === measure)!
                const agg: string | undefined = context.aggData[m.measure]
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
        setBuildTableReq((reqs) => {
            return {
                ...reqs,
                [activeTabRight]: obj,
            }
        })
        console.log(JSON.stringify(obj, null, 2))
    }

    const addNewPage = () => {
        const next = Object.keys(buildTableReq).length + 1
        setBuildTableReq((reqs) => {
            return {
                ...reqs,
                [next]: undefined,
            }
        })
    }

    const tabs = (activeTab: number) =>
        Object.entries(buildTableReq)
            .map(
                ([tab, req]): [
                    number,
                    GenerateTableDataRequest | undefined,
                ] => [parseInt(tab), req],
            )
            .map(([tab, req]) => {
                return (
                    <TabPanel index={tab} key={tab} value={activeTab}>
                        {req && <DataTable input={req} />}
                    </TabPanel>
                )
            })

    const addButton = Object.keys(buildTableReq).length > 0 && (
        <IconButton onClick={addNewPage}>
            <AddIcon />
        </IconButton>
    )

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
                        <Grid container columns={2} gap={2}>
                            <Grid>
                                <Tabs
                                    value={activeTabRight}
                                    onChange={handleActiveTabChangeRight}
                                >
                                    {Object.keys(buildTableReq).map((tab, index) => (
                                        <Tab
                                            key={tab}
                                            label={`Page ${index + 1}`}
                                            {...a11yProps(tab)}
                                        />
                                    ))}
                                    {addButton}
                                </Tabs>
                                {tabs(activeTabRight)}
                            </Grid>
                            <Grid>
                                <Tabs
                                    value={activeTabLeft}
                                    onChange={handleActiveTabChangeLeft}
                                >
                                    {Object.entries(buildTableReq).map(
                                        ([tab, data], index) =>
                                            data && (
                                                <Tab
                                                    key={tab}
                                                    label={`Page ${index + 1}`}
                                                    {...a11yProps(tab)}
                                                />
                                            ),
                                    )}
                                </Tabs>
                                {tabs(activeTabLeft)}
                            </Grid>
                        </Grid>
                    </Suspense>
                </TopBar>
            </InputStateContextProvider>
        </Box>
    )
}

export default AppWrapper

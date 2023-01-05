import { AsideList } from "../List"
import { Suspense, SyntheticEvent, useState } from "react"
import Agg from "../AggTypes"
import Accordion from "../Accordion"
import { Filters } from "../filters"
import { Filters as AddRows } from "../filters/AddRow"
import { Box, Stack } from "@mui/material"
import { InputStateUpdate, useInputs } from "../InputStateContext"
import { Overrides } from "../Overrides"
import {
    Filters as FiltersType,
    FiltersReducerDispatch,
} from "../filters/reducer"

const AggregateTab = ({
    filtersReducer,
}: {
    filtersReducer: [FiltersType, FiltersReducerDispatch]
}) => {
    const inputs = useInputs()

    const [filtersAccordionExpanded, setFiltersAccordionExpanded] =
        useState(false)
    const [addRowsAccordionExpanded, setAddRowsAccordionExpanded] =
        useState(false)

    return (
        <Stack spacing={4}>
            <AsideList
                readFrom={"fields"}
                list={"groupby"}
                title={"Group By"}
            />
            <AsideList
                readFrom={"measures"}
                list={"measuresSelected"}
                title={"Measures"}
                extras={({ field }) =>
                    inputs.canMeasureBeAggregated(field) ? (
                        <Suspense>
                            <Agg field={field} />
                        </Suspense>
                    ) : (
                        <></>
                    )
                }
            />
            <Accordion
                expanded={filtersAccordionExpanded}
                title="Filters"
                onChange={(event: SyntheticEvent, isExpanded: boolean) => {
                    setFiltersAccordionExpanded(isExpanded)
                }}
            >
                <Filters
                    component={Box}
                    reducer={filtersReducer}
                    fields={inputs.dataSet.fields}
                    onFiltersChange={(filters) => {
                        inputs.dispatcher({
                            type: InputStateUpdate.Filters,
                            data: { filters },
                        })
                    }}
                />
            </Accordion>
            <Accordion
                expanded={addRowsAccordionExpanded}
                title="Add additional rows"
                onChange={(event: SyntheticEvent, isExpanded: boolean) => {
                    setAddRowsAccordionExpanded(isExpanded)
                }}
            >
                <AddRows
                    component={Box}
                    reducer={filtersReducer}
                    fields={inputs.dataSet.fields}
                    onFiltersChange={(filters) => {
                        inputs.dispatcher({
                            type: InputStateUpdate.Filters,
                            data: { filters },
                        })
                    }}
                />
            </Accordion>
            <Overrides />
        </Stack>
    )
}

export default AggregateTab

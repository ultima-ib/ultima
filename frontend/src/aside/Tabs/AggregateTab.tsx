import { AsideList } from "../List"
import { Suspense, SyntheticEvent, useState } from "react"
import Agg from "../AggTypes"
import Accordion from "../Accordion"
import { Filters } from "../Filters"
import { AddRows, Rows } from "../AddRow"
import { Box, Checkbox, FormControlLabel, Stack } from "@mui/material"
import { InputStateUpdate, useInputs } from "../InputStateContext"
import { Overrides } from "../Overrides"
import {
    Filters as FiltersType,
    ReducerDispatch,
} from "../../utils/NestedKVStoreReducer"

const AggregateTab = ({
    filtersReducer,
    addRowsReducer,
}: {
    filtersReducer: [FiltersType, ReducerDispatch]
    addRowsReducer: [Rows, ReducerDispatch]
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
                title="Add rows"
                onChange={(event: SyntheticEvent, isExpanded: boolean) => {
                    setAddRowsAccordionExpanded(isExpanded)
                }}
            >
                <FormControlLabel
                    control={
                        <Checkbox
                            checked={inputs.additionalRows.prepare}
                            onChange={(e) => {
                                inputs.dispatcher({
                                    type: InputStateUpdate.PrepareAdditionalRows,
                                    data: {
                                        prepare: e.target.checked,
                                    },
                                })
                            }}
                        />
                    }
                    label="Prepare"
                />
                <AddRows
                    component={Box}
                    reducer={addRowsReducer}
                    onChange={(rows) => {
                        inputs.dispatcher({
                            type: InputStateUpdate.AdditionalRows,
                            data: { rows },
                        })
                    }}
                />
            </Accordion>
            <Overrides />
        </Stack>
    )
}

export default AggregateTab

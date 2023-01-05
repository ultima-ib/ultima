import { Button, Divider, IconButton, ListItem, Paper, Stack, TextField } from "@mui/material"
import { ElementType, Fragment, useEffect, useState } from "react"
import CloseIcon from "@mui/icons-material/Close"
import { ActionType, FiltersReducerDispatch } from "./reducer"
import { hasValue } from "../../utils"


interface Row {
    key: string,
    value: string,
}


const Column = (props: {
    onChange: (field: string, val: string | string[]) => void
    key: string | undefined
    value: string | undefined
}) => {
    const [field, setField] = useState<string>(props.key ?? '')
    const [val, setVal] = useState<string>(props.value ?? '')

    useEffect(() => {
        if (field !== null && val !== null) {
            props.onChange(field, val)
        }
    }, [field, val])

    return (
        <>
            <TextField
                variant="standard" label="Key"
                value={field}
                onChange={(event) => {
                    setField(event.target.value)
                }}
            />

            <TextField
                variant="standard" label="Value"
                value={val}
                onChange={(event) => {
                    setVal(event.target.value)
                }}
            />
        </>
    )
}

interface FilterListProps {
    filters: Record<number, Row>
    removeFilter: (index: number) => void
    addColumn: () => void
    onChange: (
        field: string,
        val: string | string[],
        index: number,
    ) => void
}

function RowsList(props: FilterListProps) {
    return (
        <>
            {Object.keys(props.filters)
                .map((it) => it as unknown as number)
                .map((index) => (
                    <ListItem
                        component="div"
                        key={index}
                        dense
                        disableGutters
                        sx={{
                            gap: 0.5,
                            justifyContent: "center",
                        }}
                    >
                        <IconButton
                            onClick={() => props.removeFilter(index)}
                            sx={{ p: 0, alignSelf: "last baseline" }}
                        >
                            <CloseIcon />
                        </IconButton>
                        <Column
                            onChange={(f, v) =>
                                props.onChange(f, v, index)
                            }
                            key={props.filters[index].key}
                            value={props.filters[index].value}
                        />
                    </ListItem>
                ))}
            <Button onClick={props.addColumn}>add column</Button>
        </>
    )
}

let lastUsed = 1

type RowsTy = Record<number, Record<number, Row>>
export interface FiltersProps {
    onFiltersChange: (f: RowsTy) => void
    component?: ElementType
    reducer: [RowsTy, FiltersReducerDispatch]
}

export const Filters = (props: FiltersProps) => {
    const [filters, dispatch] = props.reducer

    useEffect(() => {
        // props.onFiltersChange(filters)
        // const rows = Object.values(filters as any).map((rows) =>
        //     Object.values(rows)
        //         .filter((it) => hasValue(it.value) && hasValue(it.field))
        //         .map(({ field, value}) => ({ key: field, row: value })),
        // )
        // console.log(rows)
    }, [filters])

    const addNewRow = () => {
        lastUsed += 1
        dispatch({
            type: ActionType.NewAnd,
            index: lastUsed,
        })
    }

    const addColumn = (index: number) => {
        lastUsed += 1
        dispatch({
            type: ActionType.NewOr,
            andIndex: index,
            index: lastUsed,
        })
    }

    const removeColumn = (andIndex: number) => {
        return (index: number) => {
            dispatch({
                type: ActionType.RemoveOr,
                andIndex,
                index,
            })
            dispatch({
                type: ActionType.RemoveAnd,
                index: andIndex,
            })
        }
    }

    const updateColumn = (andIndex: number) => {
        return (
            field: string,
            value: string | string[],
            index: number,
        ) => {
            dispatch({
                type: ActionType.Update,
                andIndex,
                index,
                field,
                value,
            })
        }
    }

    const Component = props.component ?? Paper
    return (
        <>
            <Stack component={Component} spacing={1}>
                {Object.entries(filters)
                    .map(
                        ([filterNum, filter]) => [filterNum as unknown as number, filter] as const,
                    )
                    .map(([filterNum, filter]) => (
                        <Fragment key={filterNum}>
                            <RowsList
                                filters={filter}
                                removeFilter={removeColumn(filterNum)}
                                addColumn={() => addColumn(filterNum)}
                                onChange={updateColumn(filterNum)}
                            />
                            <Divider />
                        </Fragment>
                    ))}
                <Button onClick={addNewRow}>add new row</Button>
            </Stack>
        </>
    )
}

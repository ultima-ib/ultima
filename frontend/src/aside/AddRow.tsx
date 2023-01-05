import {
    Button,
    Divider,
    IconButton,
    ListItem,
    Paper,
    Stack,
    TextField,
} from "@mui/material"
import { ElementType, Fragment, useEffect, useState } from "react"
import CloseIcon from "@mui/icons-material/Close"
import { ActionType, ReducerDispatch } from "../utils/NestedKVStoreReducer"

export interface Row {
    field: string
    value: string
}

export type Rows = Record<number, Record<number, Row>>

const Column = (props: {
    onChange: (field: string, value: string | string[]) => void
    field: string | undefined
    value: string | undefined
}) => {
    const [field, setField] = useState<string>(props.field ?? "")
    const [value, setValue] = useState<string>(props.value ?? "")

    useEffect(() => {
        props.onChange(field, value)
    }, [field, value])

    return (
        <>
            <TextField
                variant="standard"
                label="Key"
                value={field}
                onChange={(event) => {
                    setField(event.target.value)
                }}
            />

            <TextField
                variant="standard"
                label="Value"
                value={value}
                onChange={(event) => {
                    setValue(event.target.value)
                }}
            />
        </>
    )
}

interface RowsListProps {
    rows: Record<number, Row>
    removeRow: (index: number) => void
    addColumn: () => void
    onChange: (field: string, value: string | string[], index: number) => void
}

function RowsList(props: RowsListProps) {
    return (
        <>
            {Object.keys(props.rows)
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
                            onClick={() => props.removeRow(index)}
                            sx={{ p: 0, alignSelf: "last baseline" }}
                        >
                            <CloseIcon />
                        </IconButton>
                        <Column
                            onChange={(f, v) => props.onChange(f, v, index)}
                            field={props.rows[index].field}
                            value={props.rows[index].value}
                        />
                    </ListItem>
                ))}
            <Button onClick={props.addColumn}>add column</Button>
        </>
    )
}

let lastUsed = 1

export interface AddRowsProps {
    onChange: (f: Rows) => void
    component?: ElementType
    reducer: [Rows, ReducerDispatch]
}

export const AddRows = (props: AddRowsProps) => {
    const [rows, dispatch] = props.reducer

    useEffect(() => {
        console.log("ya")
        props.onChange(rows)
    }, [rows])

    const addNewRow = () => {
        lastUsed += 1
        dispatch({
            type: ActionType.NewRoot,
            index: lastUsed,
        })
    }

    const addColumn = (index: number) => {
        lastUsed += 1
        dispatch({
            type: ActionType.NewChild,
            andIndex: index,
            index: lastUsed,
        })
    }

    const removeColumn = (andIndex: number) => {
        return (index: number) => {
            dispatch({
                type: ActionType.RemoveChild,
                andIndex,
                index,
            })
            dispatch({
                type: ActionType.RemoveRoot,
                index: andIndex,
            })
        }
    }

    const updateColumn = (andIndex: number) => {
        return (field: string, value: string | string[], index: number) => {
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
                {Object.entries(rows)
                    .map(
                        ([rowsNum, rowList]) =>
                            [rowsNum as unknown as number, rowList] as const,
                    )
                    .map(([rowsNum, rowList]) => (
                        <Fragment key={rowsNum}>
                            <RowsList
                                rows={rowList}
                                removeRow={removeColumn(rowsNum)}
                                addColumn={() => addColumn(rowsNum)}
                                onChange={updateColumn(rowsNum)}
                            />
                            <Divider />
                        </Fragment>
                    ))}
                <Button onClick={addNewRow}>add new row</Button>
            </Stack>
        </>
    )
}

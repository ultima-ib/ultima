import Title from "./Title";
import {List, ListItem, FormControl, Button, Autocomplete, TextField, Divider, Stack} from "@mui/material";
import React, {Dispatch, MutableRefObject, SetStateAction, useEffect, useId, useRef, useState} from "react";
import {Filter as FilterType} from "./types";

interface FilterSelectProps {
    label: string
    state: [string | undefined, Dispatch<SetStateAction<string | undefined>>]
    options: string[]
}

const FilterSelect = (props: FilterSelectProps) => {
    const [value, setValue] = props.state;

    const id = useId();

    const values = props.options
    return (
        <FormControl fullWidth variant="standard">
            <Autocomplete
                disablePortal
                id={id}
                options={values}
                onChange={(event, newValue) => {
                    setValue(newValue ?? undefined);
                }}
                value={value ?? undefined}
                renderInput={(params) => <TextField {...params} label={props.label}/>}
            />
        </FormControl>
    )
}

const Filter = (props: { onChange: (field: string, op: string, val: string) => void, fields: string[] }) => {
    const [field, setField] = useState<string | undefined>()
    const [op, setOp] = useState<string | undefined>()
    const [val, setVal] = useState<string | undefined>()
    useEffect(() => {
        if (field !== undefined && op !== undefined && val !== undefined) {
            props.onChange(field, op, val)
        }
    }, [field, op, val, props.onChange])
    return (
        <>
            <FilterSelect label="Field" state={[field, setField]} options={props.fields}/>
            <FilterSelect label="Operator" state={[op, setOp]} options={[
                'eq',
                'neq',
                'in',
                'notin',
            ]}/>
            <FilterSelect label="Value" state={[val, setVal]} options={[
                // TODO options for this will come from the API
                'three',
                'fifty',
            ]}/>
        </>
    )
}


function FilterList(props: { filters: { [p: number]: FilterType }; fields: string[] }) {
    const [filters, setFilter] = useState<number[]>([])
    const lastUsed = useRef<number>(0)

    const addNewFilter = () => {
        lastUsed.current += 1;
        setFilter((f) => [...f, lastUsed.current])
    }

    useEffect(() => {
        if (lastUsed.current === 0) {
            addNewFilter()
        }
    }, [])


    const removeFilter = (index: number) => {
        return () => {
            setFilter((filters) => filters.filter((i) => i !== index))
            delete props.filters[index]
        }
    }
    return <>
        <List dense>
            {filters.map((index) => (
                <ListItem key={index} dense disableGutters>
                    <button onClick={removeFilter(index)}>x</button>
                    <Filter onChange={(field: string, op: string, val: string) => {
                        props.filters[index] = {
                            field, op, val
                        }
                    }} fields={props.fields}/>
                </ListItem>
            ))}
        </List>
        <Button onClick={addNewFilter}>add filter</Button>
    </>;
}

export const Filters = (props: {
    filters: MutableRefObject<{ [p: number]: { [p: number]: FilterType } }>,
    fields: string[]
}) => {
    const [filters, setFilter] = useState<number[]>([])
    const lastUsed = useRef<number>(0)

    const addNewFilter = () => {
        lastUsed.current += 1;
        props.filters.current[lastUsed.current] = {}
        setFilter((f) => [...f, lastUsed.current])
    }

    useEffect(() => {
        if (lastUsed.current === 0) {
            addNewFilter()
        }
    }, [])

    return (
        <section>
            <Title content='Filters'/>
            <Stack spacing={2}>
                {
                    filters.map((filter) => (
                        <React.Fragment key={filter}>
                            <FilterList filters={props.filters.current[filter]} fields={props.fields}/>
                            <Divider/>
                        </React.Fragment>
                    ))
                }
            </Stack>
            <Button onClick={addNewFilter}>add and filter</Button>
        </section>
    )
}

import Title from "./Title";
import {
    Autocomplete,
    Box,
    Button,
    Checkbox,
    Divider,
    IconButton,
    ListItem,
    Paper,
    Stack,
    TextField
} from "@mui/material";
import {
    Dispatch,
    Fragment,
    SetStateAction,
    Suspense,
    useDeferredValue,
    useEffect,
    useId,
    useRef,
    useState,
    useTransition
} from "react";
import {Filter as FilterType} from "./types";
import {useFilterColumns} from "../api/hooks";
import CloseIcon from '@mui/icons-material/Close';
import {InputStateUpdate, useInputs} from "./InputStateContext";

import CheckBoxOutlineBlankIcon from '@mui/icons-material/CheckBoxOutlineBlank';
import CheckBoxIcon from '@mui/icons-material/CheckBox';

const icon = <CheckBoxOutlineBlankIcon fontSize="small"/>;
const checkedIcon = <CheckBoxIcon fontSize="small"/>;

interface FilterSelectProps {
    label: string
    state: [string | string[] | null, Dispatch<SetStateAction<string | null>> | Dispatch<SetStateAction<string | string[] | null>>]
    options: string[]
    inputValue?: string
    onInputChange?: (value: string) => void
    disabled?: boolean
    filterOptions?: (o: string[]) => string[]
    multiple?: boolean
}

const FilterSelect = (props: FilterSelectProps) => {
    const [value, setValue] = props.state;

    const id = useId();

    const values = props.options
    const multiple = props.multiple ?? false;
    return (
        <Autocomplete
            multiple={multiple}
            disablePortal
            disabled={props.disabled ?? false}
            filterOptions={props.filterOptions}
            id={id}
            options={values}
            renderOption={(props, option, {selected}) => (
                <li {...props}>
                    {multiple && <Checkbox
                        icon={icon}
                        checkedIcon={checkedIcon}
                        style={{marginRight: 8}}
                        checked={selected}
                    />}
                    {option}
                </li>
            )}
            onChange={(event, newValue) => {
                setValue(newValue as unknown as any ?? null);
            }}
            inputValue={props.inputValue}
            onInputChange={(event, value) => {
                props.onInputChange?.(value)
            }}
            sx={{width: '100%'}}
            value={value ? value : (multiple ? [] : null)}
            renderInput={(params) => <TextField {...params} variant="standard" label={props.label}/>}
        />
    )
}

const Filter = (props: { onChange: (field: string, op: string, val: string | string[]) => void, fields: string[] }) => {
    const [field, setField] = useState<string | null>(null)
    const [op, setOp] = useState<string | null>(null)
    const [val, setVal] = useState<string | string[] | null>(null)

    const [pending, startTransition] = useTransition()

    useEffect(() => {
        if (field !== null && op !== null && val !== null) {
            props.onChange(field, op, val)
        }
    }, [field, op, val])
    const [valueSearchInput, setValueSearchInput] = useState('');

    const deferredSearchInput = useDeferredValue(valueSearchInput)
    const searchResults = useFilterColumns(field ?? '', deferredSearchInput)

    return (
        <>
            <FilterSelect label="Field" state={[field, (v: unknown) => startTransition(() => {
                setField(v as string | null)
                setVal(null)
            })]} options={props.fields}/>
            <FilterSelect label="Operator" state={[op, setOp]} options={[
                'Eq',
                'Neq',
                'In',
                'NotIn',
            ]}/>
            <Suspense fallback={"Loading..."}>
                <FilterSelect
                    filterOptions={(x) => x}
                    disabled={pending}
                    label="Value"
                    state={[val, setVal]}
                    options={searchResults}
                    inputValue={valueSearchInput}
                    onInputChange={(value) => setValueSearchInput(value)}
                    multiple={op === 'In' || op === 'NotIn'}
                />
            </Suspense>
        </>
    )
}


function FilterList(props: { filters: { [p: number]: FilterType }, fields: string[], onRemove: () => void, filterNum: number }) {
    const inputs = useInputs();
    const [filters, setFilter] = useState<number[]>(Object.keys(props.filters) as unknown as number[])
    const lastUsed = useRef<number>(filters.length)

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
            delete inputs.filters[props.filterNum][index]
            inputs.dispatcher({
                type: InputStateUpdate.Filters,
                data: {
                    filters: {
                        ...inputs.filters
                    }
                }
            })
            props.onRemove()
        }
    }
    return <>
        {filters.map((index) => (
            <ListItem component='div' key={index} dense disableGutters sx={{
                gap: 0.5,
                justifyContent: 'center',
                // height: '100%'
            }}>
                <IconButton onClick={removeFilter(index)} sx={{p: 0, alignSelf: 'last baseline'}}>
                    <CloseIcon/>
                </IconButton>
                <Filter onChange={(field, op, val) => {
                    inputs.dispatcher({
                        type: InputStateUpdate.Filters,
                        data: {
                            filters: {
                                ...inputs.filters,
                                [props.filterNum]: {
                                    ...inputs.filters[props.filterNum],
                                    [index]: {
                                        field, op, value: val
                                    }
                                }
                            }
                        }
                    })
                }} fields={props.fields}/>
            </ListItem>
        ))}
        <Button onClick={addNewFilter}>add filter</Button>
    </>;
}

export const Filters = () => {
    const inputs = useInputs();
    const lastUsed = useRef<number>(Object.keys(inputs.filters).length)

    const addNewFilter = () => {
        lastUsed.current += 1;
        inputs.dispatcher({
            type: InputStateUpdate.Filters,
            data: {
                filters: {
                    ...inputs.filters,
                    [lastUsed.current]: {}
                }
            }
        })
    }

    const removeFilter = (filter: number) => {
        return () => {
            if (Object.keys(inputs.filters[filter]).length === 0) {
                const copy = {...inputs.filters}
                delete copy[filter]
                inputs.dispatcher({
                    type: InputStateUpdate.Filters,
                    data: {
                        filters: copy
                    }
                })
            }
        }
    }


    useEffect(() => {
        if (lastUsed.current === 0) {
            addNewFilter()
        }
    }, [])

    return (
        <Box sx={{height: '70%'}}>
            <Title content='Filters'/>
            <Stack component={Paper} spacing={1} sx={{overflowX: 'hidden', height: '100%'}}>
                {
                    Object.entries(inputs.filters).map(([filterNum, filter]) => (
                        <Fragment key={filterNum}>
                            <FilterList
                                filterNum={filterNum as unknown as number}
                                filters={filter}
                                fields={inputs.dataSet.fields}
                                onRemove={removeFilter(filterNum as unknown as number)}
                            />
                            <Divider/>
                        </Fragment>
                    ))
                }
                <Button onClick={addNewFilter}>add and filter</Button>
            </Stack>
        </Box>
    )
}

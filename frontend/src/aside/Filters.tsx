import Title from "./Title";
import {
    ListItem,
    Button,
    Autocomplete,
    TextField,
    Divider,
    Stack,
    Box,
    BoxProps, IconButton
} from "@mui/material";
import {
    Fragment,
    Dispatch,
    MutableRefObject,
    SetStateAction,
    useEffect,
    useId,
    useRef,
    useState,
    Suspense,
    useTransition, useDeferredValue
} from "react";
import {Filter as FilterType} from "./types";
import {useFilterColumns} from "../api/hooks";
import CloseIcon from '@mui/icons-material/Close';

interface FilterSelectProps {
    label: string
    state: [string | null, Dispatch<SetStateAction<string | null>>]
    options: string[]
    inputValue?: string
    onInputChange?: (value: string) => void
    disabled?: boolean
}

const FilterSelect = (props: FilterSelectProps) => {
    const [value, setValue] = props.state;

    const id = useId();

    const values = props.options
    return (
        <Autocomplete
            disablePortal
            disabled={props.disabled ?? false}
            id={id}
            options={values}
            onChange={(event, newValue) => {
                setValue(newValue ?? null);
            }}
            inputValue={props.inputValue}
            onInputChange={(event, value) => {
                props.onInputChange?.(value)
            }}
            value={value}
            sx={{width: '100%'}}
            renderInput={(params) => <TextField {...params} variant="standard" label={props.label}/>}
        />
    )
}

const Filter = (props: { onChange: (field: string, op: string, val: string) => void, fields: string[] }) => {
    const [field, setField] = useState<string | null>(null)
    const [op, setOp] = useState<string | null>(null)
    const [val, setVal] = useState<string | null>(null)

    const [pending, startTransition] = useTransition()

    useEffect(() => {
        if (field !== null && op !== null && val !== null) {
            props.onChange(field, op, val)
        }
    }, [field, op, val, props.onChange])
    const [valueSearchInput, setValueSearchInput] = useState('');

    const deferredSearchInput = useDeferredValue(valueSearchInput)
    const searchResults = useFilterColumns(field ?? '', deferredSearchInput)

    return (
        <>
            <FilterSelect label="Field" state={[field, (v) => startTransition(() => {
                setField(v)
                setValueSearchInput('')
                setVal(null)
            })]} options={props.fields}/>
            <FilterSelect label="Operator" state={[op, setOp]} options={[
                'eq',
                'neq',
                'in',
                'notin',
            ]}/>
            <Suspense fallback={"Loading..."}>
                <FilterSelect
                    disabled={pending}
                    label="Value"
                    state={[val, (value) => startTransition(() => setVal(value))]} options={searchResults}
                    inputValue={valueSearchInput}
                    onInputChange={(value) => startTransition(() => setValueSearchInput(value))}
                />
            </Suspense>
        </>
    )
}


function FilterList(props: { filters: { [p: number]: FilterType }, fields: string[], onRemove: () => void }) {
    const [filters, setFilter] = useState<number[]>( Object.keys(props.filters) as unknown as number[])
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
            delete props.filters[index]
            props.onRemove()
        }
    }
    return <>
        <Box>
            {filters.map((index) => (
                <ListItem component='div' key={index} dense disableGutters sx={{
                    gap: 0.5,
                    justifyContent: 'center'
                }}>
                    <IconButton onClick={removeFilter(index)} sx={{p: 0, alignSelf: 'last baseline'}}>
                        <CloseIcon />
                    </IconButton>
                    <Filter onChange={(field: string, op: string, val: string) => {
                        props.filters[index] = {
                            field, op, val
                        }
                    }} fields={props.fields}/>
                </ListItem>
            ))}
        </Box>
        <Button onClick={addNewFilter}>add filter</Button>
    </>;
}

export const Filters = (props: {
    filters: MutableRefObject<{ [p: number]: { [p: number]: FilterType } }>,
    fields: string[]
} & BoxProps) => {
    const [filters, setFilter] = useState<number[]>(Object.keys(props.filters.current) as unknown as number[])
    const lastUsed = useRef<number>(filters.length)

    const addNewFilter = () => {
        lastUsed.current += 1;
        props.filters.current[lastUsed.current] = {}
        setFilter((f) => [...f, lastUsed.current])
    }

    const removeFilter = (filter: number) => {
        return () => {
            if (Object.keys(props.filters.current[filter]).length === 0) {
                setFilter((filters) => filters.filter((i) => i !== filter))
                delete props.filters.current[filter]
            }
        }
    }


    useEffect(() => {
        if (lastUsed.current === 0) {
            addNewFilter()
        }
    }, [])

    return (
        <Box sx={{overflow: 'scroll'}}>
            <Title content='Filters'/>
            <Stack spacing={1} sx={{overflow: 'scroll', height: '8rem'}}>
                {
                    filters.map((filter) => (
                        <Fragment key={filter}>
                            <FilterList filters={props.filters.current[filter]} fields={props.fields} onRemove={removeFilter(filter)}/>
                            <Divider/>
                        </Fragment>
                    ))
                }
            </Stack>
            <Button onClick={addNewFilter}>add and filter</Button>
        </Box>
    )
}

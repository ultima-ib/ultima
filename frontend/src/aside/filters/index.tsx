import {
    Autocomplete,
    Button,
    Checkbox,
    Divider,
    IconButton,
    ListItem,
    Paper,
    Stack,
    TextField,
} from "@mui/material"
import {
    Dispatch,
    ElementType,
    Fragment,
    SetStateAction,
    Suspense,
    useDeferredValue,
    useEffect,
    useId,
    useReducer,
    useState,
    useTransition,
} from "react"
import { Filter as FilterType } from "../types"
import { useFilterColumns } from "../../api/hooks"
import CloseIcon from "@mui/icons-material/Close"
import CheckBoxOutlineBlankIcon from "@mui/icons-material/CheckBoxOutlineBlank"
import CheckBoxIcon from "@mui/icons-material/CheckBox"
import { ActionType, reducer, Filters as FiltersType } from "./reducer"

const icon = <CheckBoxOutlineBlankIcon fontSize="small" />
const checkedIcon = <CheckBoxIcon fontSize="small" />

interface FilterSelectProps {
    label: string
    state: [
        string | string[] | null,
        (
            | Dispatch<SetStateAction<string | null>>
            | Dispatch<SetStateAction<string | string[] | null>>
        ),
    ]
    options: string[]
    inputValue?: string
    onInputChange?: (value: string) => void
    disabled?: boolean
    filterOptions?: (o: string[]) => string[]
    multiple?: boolean
}

const FilterSelect = (props: FilterSelectProps) => {
    const [value, setValue] = props.state

    const id = useId()

    const values = props.options
    const multiple = props.multiple ?? false
    return (
        <Autocomplete
            multiple={multiple}
            disableCloseOnSelect={multiple}
            disablePortal
            disabled={props.disabled ?? false}
            filterOptions={props.filterOptions}
            id={id}
            options={values}
            renderOption={(renderProps, option, { selected }) => (
                <li {...renderProps}>
                    {multiple && (
                        <Checkbox
                            icon={icon}
                            checkedIcon={checkedIcon}
                            style={{ marginRight: 8 }}
                            checked={selected}
                        />
                    )}
                    {option}
                </li>
            )}
            onChange={(event, newValue) => {
                setValue((newValue as unknown as string | undefined) ?? null)
            }}
            inputValue={props.inputValue}
            onInputChange={(event, newValue) => {
                props.onInputChange?.(newValue)
            }}
            sx={{ width: "100%" }}
            value={value ? value : multiple ? [] : null}
            renderInput={(params) => (
                <TextField {...params} variant="standard" label={props.label} />
            )}
        />
    )
}

const Filter = (props: {
    onChange: (field: string, op: string, val: string | string[]) => void
    fields: string[]
}) => {
    const [field, setField] = useState<string | null>(null)
    const [op, setOp] = useState<string | null>(null)
    const [val, setVal] = useState<string | string[] | null>(null)

    const [pending, startTransition] = useTransition()

    useEffect(() => {
        if (field !== null && op !== null && val !== null) {
            props.onChange(field, op, val)
        }
    }, [field, op, val])
    const [valueSearchInput, setValueSearchInput] = useState("")

    const deferredSearchInput = useDeferredValue(valueSearchInput)
    const searchResults = useFilterColumns(field ?? "", deferredSearchInput)

    return (
        <>
            <FilterSelect
                label="Field"
                state={[
                    field,
                    (v: unknown) =>
                        startTransition(() => {
                            setField(v as string | null)
                            setVal(null)
                        }),
                ]}
                options={props.fields}
            />
            <FilterSelect
                label="Operator"
                state={[op, setOp]}
                options={["Eq", "Neq", "In", "NotIn"]}
            />
            <Suspense fallback={"Loading..."}>
                <FilterSelect
                    filterOptions={(x) => x}
                    disabled={pending}
                    label="Value"
                    state={[val, setVal]}
                    options={searchResults}
                    inputValue={valueSearchInput}
                    onInputChange={(value) => setValueSearchInput(value)}
                    multiple={op === "In" || op === "NotIn"}
                />
            </Suspense>
        </>
    )
}

interface FilterListProps {
    filters: Record<number, FilterType>
    fields: string[]
    removeFilter: (index: number) => void
    addFilter: () => void
    onChange: (
        field: string,
        op: string,
        val: string | string[],
        index: number,
    ) => void
}

function FilterList(props: FilterListProps) {
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
                        <Filter
                            onChange={(f, o, v) =>
                                props.onChange(f, o, v, index)
                            }
                            fields={props.fields}
                        />
                    </ListItem>
                ))}
            <Button onClick={props.addFilter}>add filter</Button>
        </>
    )
}

let lastUsed = 1

export const Filters = (props: {
    fields?: string[]
    onFiltersChange: (f: FiltersType) => void
    component?: ElementType
}) => {
    const [filters, dispatch] = useReducer(reducer, {
        0: {
            1: {},
        },
    })

    useEffect(() => {
        props.onFiltersChange(filters)
    }, [filters])
    const addNewFilter = () => {
        lastUsed += 1
        dispatch({
            type: ActionType.NewAnd,
            index: lastUsed,
        })
    }

    const addNewOrFilter = (index: number) => {
        lastUsed += 1
        dispatch({
            type: ActionType.NewOr,
            andIndex: index,
            index: lastUsed,
        })
    }

    const removeOrFilter = (andIndex: number) => {
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

    const updateFilter = (andIndex: number) => {
        return (
            field: string,
            op: string,
            value: string | string[],
            index: number,
        ) => {
            dispatch({
                type: ActionType.Update,
                andIndex,
                index,
                field,
                op,
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
                        ([filterNum, filter]): [
                            number,
                            FiltersType[number],
                        ] => [filterNum as unknown as number, filter],
                    )
                    .map(([filterNum, filter]) => (
                        <Fragment key={filterNum}>
                            <FilterList
                                filters={filter}
                                fields={props.fields ?? []}
                                removeFilter={removeOrFilter(filterNum)}
                                addFilter={() => addNewOrFilter(filterNum)}
                                onChange={updateFilter(filterNum)}
                            />
                            <Divider />
                        </Fragment>
                    ))}
                <Button onClick={addNewFilter}>add and filter</Button>
            </Stack>
        </>
    )
}

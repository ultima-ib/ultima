import {
    ChangeEvent,
    PropsWithChildren,
    Suspense,
    SyntheticEvent,
    ElementType,
    useDeferredValue,
    useEffect,
    useState,
} from "react"
import type { DraggableLocation, DropResult } from "@hello-pangea/dnd"
import { DragDropContext } from "@hello-pangea/dnd"
import { reorderQuoteMap } from "./reorder"
import {
    Accordion as MuiAccordion,
    AccordionDetails,
    AccordionProps,
    AccordionSummary,
    Box,
    BoxProps,
    Checkbox,
    FormControlLabel,
    IconButton,
    Stack,
    StackProps,
    Tab,
    Tabs,
    TextField,
    Paper,
    Divider,
} from "@mui/material"
import QuoteList, { ListItemExtras } from "./list"
import Title from "./Title"
import { Filters } from "./filters"
import type { DataSet } from "./types"
import Agg from "./AggTypes"
import { InputStateUpdate, useInputs } from "./InputStateContext"
import ExpandMoreIcon from "@mui/icons-material/ExpandMore"
import { Resizable as ReResizable } from "re-resizable"
import DeleteIcon from "@mui/icons-material/Close"
import { Overrides } from "./Overrides"
import { Templates } from "./Templates"
import { useTheme } from "@mui/material/styles"
import CalcParams from "./CalcParams"

interface ResizableProps {
    top?: boolean
    right?: boolean
    bottom?: boolean
    left?: boolean
    topRight?: boolean
    bottomRight?: boolean
    bottomLeft?: boolean
    topLeft?: boolean
    defaultHeight?: string
    defaultWidth: string
}

const Resizable = (props: PropsWithChildren<ResizableProps>) => {
    const theme = useTheme()

    return (
        <ReResizable
            handleComponent={{
                right: (
                    <div
                        style={{
                            background: theme.palette.text.secondary,
                            height: "100%",
                            width: "1px",
                        }}
                    />
                ),
                bottom: (
                    <div
                        style={{
                            background: theme.palette.text.secondary,
                            height: "1px",
                            width: "100%",
                        }}
                    />
                ),
            }}
            minWidth="300px"
            defaultSize={{
                width: props.defaultWidth,
                height: props.defaultHeight ?? "100%",
            }}
            enable={{
                top: props.top ?? false,
                right: props.right ?? false,
                bottom: props.bottom ?? false,
                left: props.left ?? false,
                topRight: props.topRight ?? false,
                bottomRight: props.bottomRight ?? false,
                bottomLeft: props.bottomLeft ?? false,
                topLeft: props.topLeft ?? false,
            }}
            style={{
                display: "flex",
                gap: "0.5em",
                minWidth: "300px",
                backgroundColor: theme.palette.background.paper,
            }}
        >
            {props.children}
        </ReResizable>
    )
}

interface TabPanelProps extends BoxProps {
    index: number
    value: number
}

function TabPanel(props: TabPanelProps) {
    const { children, value, index, ...other } = props

    return (
        <Box
            role="tabPanel"
            hidden={value !== index}
            id={`tabPanel-${index}`}
            aria-labelledby={`tab-${index}`}
            {...other}
        >
            {value === index && children}
        </Box>
    )
}

function a11yProps(index: number) {
    return {
        id: `tab-${index}`,
        "aria-controls": `tabPanel-${index}`,
    }
}

interface ColumnProps {
    title?: string
    fields: string[]
    listId: string
    extras?: ListItemExtras
    onListItemClick?: (field: string) => void
    multiColumn?: boolean
    titleComponent?: ElementType
}

export function Column({
    title,
    fields,
    listId,
    extras,
    onListItemClick,
    multiColumn,
    titleComponent,
    ...stack
}: ColumnProps & StackProps) {
    return (
        <Stack spacing={2} alignItems="center" {...stack}>
            {title && (
                <Title content={title} component={titleComponent ?? Paper} />
            )}
            <QuoteList
                listId={listId}
                listType="QUOTE"
                fields={fields}
                extras={extras}
                onListItemClick={onListItemClick}
                multiColumn={multiColumn ?? false}
            />
        </Stack>
    )
}

const Accordion = ({
    title,
    children,
    hideExpandButton,
    ...rest
}: AccordionProps & { title: string; hideExpandButton?: boolean }) => (
    <MuiAccordion {...rest}>
        <AccordionSummary
            expandIcon={!hideExpandButton && <ExpandMoreIcon />}
            sx={{ my: 0 }}
        >
            {title}
        </AccordionSummary>
        <AccordionDetails
            sx={{
                minHeight: "100px",
                ".MuiAccordionDetails-root": {
                    px: 1,
                },
                ".MuiListItemButton-root": {
                    px: 1,
                },
            }}
        >
            {children}
        </AccordionDetails>
    </MuiAccordion>
)

const AccordionColumn = ({
    title,
    expanded,
    onAccordionStateChange: onChange,
    ...rest
}: ColumnProps & {
    expanded?: boolean
    onAccordionStateChange?: (event: SyntheticEvent, expanded: boolean) => void
}) => {
    return (
        <Accordion
            expanded={expanded}
            title={title ?? ""}
            onChange={onChange}
            sx={{ width: "100%" }}
        >
            <Column {...rest} />
        </Accordion>
    )
}

const SearchBox = (props: { onChange: (text: string) => void }) => {
    const [searchText, setSearchText] = useState("")
    const onSearchTextChange = (event: ChangeEvent<HTMLInputElement>) => {
        setSearchText(event.target.value)
    }
    const deferredSearchText = useDeferredValue(searchText)
    useEffect(() => {
        props.onChange(deferredSearchText)
    }, [deferredSearchText])

    return (
        <TextField
            value={searchText}
            onChange={onSearchTextChange}
            label="Search"
            sx={{ my: 1, mx: 1 }}
            variant="filled"
        ></TextField>
    )
}

const DeleteButton = (props: {
    field: string
    from: keyof Omit<DataSet, "calcParams">
}) => {
    const inputs = useInputs()

    const onDelete = () => {
        const returnTo =
            props.from === "measuresSelected" ? "measures" : "fields"
        const fromList = inputs.dataSet[props.from].filter(
            (it) => it !== props.field,
        )
        const orgList = inputs.dataSet[returnTo]
        const toList = orgList.includes(props.field)
            ? orgList
            : [...orgList, props.field]

        inputs.dispatcher({
            type: InputStateUpdate.DataSet,
            data: {
                // @ts-expect-error signature mismatch
                dataSet: {
                    [props.from]: fromList,
                    [returnTo]: toList,
                },
            },
        })
    }

    return (
        <IconButton onClick={onDelete}>
            <DeleteIcon />
        </IconButton>
    )
}

const Aside = () => {
    const inputs = useInputs()
    const columns = inputs.dataSet
    const onDragEnd = (result: DropResult): void => {
        const source: DraggableLocation = result.source
        // dragged nowhere, bail
        if (result.destination === null) {
            return
        }
        const destination: DraggableLocation = result.destination

        // did not move anywhere - can bail early
        if (
            source.droppableId === destination.droppableId &&
            source.index === destination.index
        ) {
            return
        }

        if (
            destination.droppableId === "fields" ||
            destination.droppableId === "measures"
        ) {
            return
        }
        const data = reorderQuoteMap(columns, source, destination)

        inputs.dispatcher({
            type: InputStateUpdate.DataSet,
            data: {
                dataSet: {
                    ...inputs.dataSet,
                    ...data,
                },
            },
        })
    }

    const [activeTab, setActiveTab] = useState(0)

    const [searchValue, setSearchValue] = useState<string | undefined>(
        undefined,
    )

    const [groupByAccordionExpanded, setGroupByAccordionExpanded] =
        useState(false)

    const doSearch = (orElse: string[]) => {
        if (searchValue) {
            const results = orElse.filter((it) =>
                it.toLowerCase().includes(searchValue.toLowerCase()),
            )
            if (results.length >= 0) {
                return results
            } else {
                return []
            }
        } else {
            return orElse
        }
    }
    const handleActiveTabChange = (event: SyntheticEvent, newValue: number) => {
        setActiveTab(newValue)
    }

    const addToList = (list: "measuresSelected" | "groupby", what: string) => {
        if (columns[list].includes(what)) {
            return
        }
        inputs.dispatcher({
            type: InputStateUpdate.DataSet,
            data: {
                // @ts-expect-error mismatched signature
                dataSet: {
                    [list]: [...columns[list], what],
                },
            },
        })
        if (list === "groupby") {
            setGroupByAccordionExpanded(true)
        }
    }
    return (
        <DragDropContext onDragEnd={onDragEnd}>
            <Resizable right defaultWidth="40%">
                <Stack sx={{ width: "40%" }}>
                    <SearchBox onChange={(v) => setSearchValue(v)} />
                    <Column
                        title="Measures"
                        fields={doSearch(columns.measures)}
                        listId="measures"
                        sx={{ height: "45%" }}
                        onListItemClick={(field) => {
                            addToList("measuresSelected", field)
                        }}
                    />
                    <Column
                        title="Fields"
                        fields={doSearch(columns.fields)}
                        listId="fields"
                        sx={{ height: "45%" }}
                        onListItemClick={(field) => {
                            addToList("groupby", field)
                        }}
                    />
                </Stack>
                <Divider orientation="vertical" />
                <Stack sx={{ width: "60%", height: "100%" }}>
                    <Suspense fallback="Loading templates....">
                        <Templates />
                    </Suspense>
                    <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
                        <Tabs
                            value={activeTab}
                            onChange={handleActiveTabChange}
                        >
                            <Tab label="Aggregate" {...a11yProps(0)} />
                            <Tab label="Params" {...a11yProps(1)} />
                        </Tabs>
                    </Box>
                    <TabPanel
                        value={activeTab}
                        index={0}
                        sx={{ height: "100%", overflow: "auto" }}
                    >
                        <AccordionColumn
                            expanded={groupByAccordionExpanded}
                            title="Group By"
                            fields={columns.groupby}
                            listId="groupby"
                            multiColumn
                            extras={({ field }) => (
                                <DeleteButton field={field} from="groupby" />
                            )}
                            onAccordionStateChange={(
                                event: SyntheticEvent,
                                isExpanded: boolean,
                            ) => {
                                setGroupByAccordionExpanded(isExpanded)
                            }}
                        />
                        <Overrides />
                        <Resizable
                            bottom
                            defaultHeight="20%"
                            defaultWidth="100%"
                        >
                            <Column
                                title="Measures"
                                fields={columns.measuresSelected}
                                listId="measuresSelected"
                                titleComponent={Box}
                                sx={{
                                    width: "100%",
                                    overflowX: "scroll",
                                }}
                                extras={({ field }) => (
                                    <>
                                        {inputs.canMeasureBeAggregated(
                                            field,
                                        ) && (
                                            <Suspense>
                                                <Agg field={field} />
                                            </Suspense>
                                        )}
                                        <DeleteButton
                                            field={field}
                                            from="measuresSelected"
                                        />
                                    </>
                                )}
                            />
                        </Resizable>
                        <Box sx={{ width: "100%" }}>
                            <Filters
                                component={Box}
                                fields={columns.fields}
                                onFiltersChange={(filters) => {
                                    inputs.dispatcher({
                                        type: InputStateUpdate.Filters,
                                        data: {
                                            filters,
                                        },
                                    })
                                }}
                            />
                        </Box>
                    </TabPanel>
                    <TabPanel
                        value={activeTab}
                        index={1}
                        sx={{ height: "100%", overflow: "hidden" }}
                    >
                        <Box>
                            <FormControlLabel
                                control={
                                    <Checkbox
                                        checked={inputs.hideZeros}
                                        onChange={(e) =>
                                            inputs.dispatcher({
                                                type: InputStateUpdate.HideZeros,
                                                data: {
                                                    hideZeros: e.target.checked,
                                                },
                                            })
                                        }
                                    />
                                }
                                label="Hide Zeros"
                            />

                            <FormControlLabel
                                control={
                                    <Checkbox
                                        checked={inputs.totals}
                                        onChange={(e) =>
                                            inputs.dispatcher({
                                                type: InputStateUpdate.Total,
                                                data: {
                                                    totals: e.target.checked,
                                                },
                                            })
                                        }
                                    />
                                }
                                label="Totals"
                            />
                        </Box>
                        <Box sx={{ overflowY: "auto", maxHeight: "80vh" }}>
                            <CalcParams />
                        </Box>
                    </TabPanel>
                </Stack>
            </Resizable>
        </DragDropContext>
    )
}
export default Aside

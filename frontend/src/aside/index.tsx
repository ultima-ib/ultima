import {
    ChangeEvent,
    PropsWithChildren,
    Suspense,
    SyntheticEvent,
    useDeferredValue,
    useEffect,
    useState, ReactElement,
} from "react"
import {
    Accordion as MuiAccordion,
    AccordionDetails,
    AccordionProps,
    AccordionSummary,
    Box,
    BoxProps,
    Checkbox,
    FormControlLabel,
    Stack,
    Tab,
    Tabs,
    TextField,
} from "@mui/material"
import { Filters } from "./filters"
import Agg from "./AggTypes"
import TheList from "./List"
import { InputStateUpdate, useInputs } from "./InputStateContext"
import ExpandMoreIcon from "@mui/icons-material/ExpandMore"
import { Resizable as ReResizable } from "re-resizable"
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
            sx={{ my: 1, mx: 1, width: "100%" }}
            variant="filled"
        />
    )
}

const Aside = () => {
    const inputs = useInputs()
    const [activeTab, setActiveTab] = useState(0)

    const [filtersAccordionExpanded, setFiltersAccordionExpanded] = useState(false)

    const handleActiveTabChange = (event: SyntheticEvent, newValue: number) => {
        setActiveTab(newValue)
    }

    return (
        <Resizable right defaultWidth="40%">
            <Stack sx={{ width: "100%", height: "100%" }}>
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
                    <Stack spacing={4}>
                        <AsideList readFrom={"fields"} list={"groupby"} title={"Group By"} />
                        <AsideList
                            readFrom={"measures"}
                            list={"measuresSelected"}
                            title={"Measures"}
                            extras={({ field }) => inputs.canMeasureBeAggregated(field) ? (
                                <Suspense>
                                    <Agg field={field} />
                                </Suspense>
                            ) : <></>}
                        />
                        <Accordion expanded={filtersAccordionExpanded} title="Filters" onChange={(
                            event: SyntheticEvent,
                            isExpanded: boolean,
                        ) => {
                            setFiltersAccordionExpanded(isExpanded)
                        }}>
                            <Filters
                                component={Box}
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
    )
}

function AsideList({ readFrom, list, title, extras }: {
    readFrom: "fields" | "measures",
    list: "measuresSelected" | "groupby",
    title: string,
    extras?: (v: { field: string }) => ReactElement
}) {

    const [searchValue, setSearchValue] = useState<string | undefined>()

    const [accordionExpanded, setAccordionExpanded] = useState(false)

    return (
        <Accordion title={title} expanded={accordionExpanded} onChange={(
            event: SyntheticEvent,
            isExpanded: boolean,
        ) => {
            setAccordionExpanded(isExpanded)
        }}>
            <SearchBox onChange={(v) => setSearchValue(v)} />
            <TheList readFrom={readFrom} list={list} extras={extras} searchValue={searchValue ?? ""} />
        </Accordion>
    )
}

export default Aside

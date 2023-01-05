import {
    PropsWithChildren,
    Suspense,
    SyntheticEvent,
    useReducer,
    useState,
} from "react"
import { Box, Stack, Tab, Tabs } from "@mui/material"
import AggregateTab from "./Tabs/AggregateTab"
import { Resizable as ReResizable } from "re-resizable"
import { Templates } from "./Templates"
import { useTheme } from "@mui/material/styles"
import { TabPanel, a11yProps } from "./Tabs/TabPanel"
import ParamsTab from "./Tabs/ParamsTab"
import { ActionType, reducer } from "./filters/reducer"

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

const Aside = () => {
    const [activeTab, setActiveTab] = useState(0)

    const [filters, dispatch] = useReducer(reducer, {})
    const [addRows, addRowsDispatch] = useReducer(reducer, {})

    const handleActiveTabChange = (event: SyntheticEvent, newValue: number) => {
        setActiveTab(newValue)
    }

    return (
        <Resizable right defaultWidth="40%">
            <Stack sx={{ width: "100%", height: "100%" }}>
                <Suspense fallback="Loading templates....">
                    <Templates
                        setFilters={(f) =>
                            dispatch({
                                type: ActionType.Set,
                                filters: f,
                            })
                        }
                    />
                </Suspense>
                <Box sx={{ borderBottom: 1, borderColor: "divider" }}>
                    <Tabs value={activeTab} onChange={handleActiveTabChange}>
                        <Tab label="Aggregate" {...a11yProps(0)} />
                        <Tab label="Params" {...a11yProps(1)} />
                    </Tabs>
                </Box>
                <TabPanel
                    value={activeTab}
                    index={0}
                    sx={{ height: "100%", overflow: "auto" }}
                >
                    <AggregateTab filtersReducer={[filters, dispatch]} />
                </TabPanel>
                <TabPanel
                    value={activeTab}
                    index={1}
                    sx={{ height: "100%", overflow: "hidden" }}
                >
                    <ParamsTab />
                </TabPanel>
            </Stack>
        </Resizable>
    )
}

export default Aside

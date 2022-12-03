import {    Box,    BoxProps,} from "@mui/material"

interface TabPanelProps extends BoxProps {
    index: number
    value: number
}

export function TabPanel(props: TabPanelProps) {
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

export function a11yProps(index: number) {
    return {
        id: `tab-${index}`,
        "aria-controls": `tabPanel-${index}`,
    }
}

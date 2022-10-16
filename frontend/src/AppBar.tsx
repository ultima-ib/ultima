import {
    AppBar,
    Box,
    Toolbar,
    Slide,
    Button,
    IconButton,
    Menu,
    MenuItem,
} from "@mui/material"
import useScrollTrigger from "@mui/material/useScrollTrigger"
import { PropsWithChildren, useRef, useState, MouseEvent, useId } from "react"
import { useTheme } from "@mui/material/styles"
import PlayArrowIcon from "@mui/icons-material/PlayArrow"
import CompareIcon from "@mui/icons-material/Compare"
import Brightness4Icon from "@mui/icons-material/Brightness4"
import Brightness7Icon from "@mui/icons-material/Brightness7"
import ContentCopyIcon from "@mui/icons-material/ContentCopy"
import { useColorMode } from "./App"

interface CopyRequestToClipboardMenuProps {
    copyMainTable: () => void
    copyComparisonTable: () => void
}

function CopyRequestToClipboardMenu(props: CopyRequestToClipboardMenuProps) {
    const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null)
    const open = anchorEl !== null
    const handleClick = (event: MouseEvent<HTMLButtonElement>) => {
        setAnchorEl(event.currentTarget)
    }
    const handleClose = () => {
        setAnchorEl(null)
    }

    const copyMainTable = () => {
        props.copyMainTable()
        handleClose()
    }
    const copyComparisonTable = () => {
        props.copyComparisonTable()
        handleClose()
    }

    const id = useId()

    return (
        <div>
            <Button
                id={id}
                sx={{ mr: 2 }}
                startIcon={<ContentCopyIcon />}
                color="inherit"
                aria-haspopup="true"
                aria-expanded={open ? "true" : undefined}
                onClick={handleClick}
            >
                Copy request
            </Button>
            <Menu
                anchorEl={anchorEl}
                open={open}
                onClose={handleClose}
                MenuListProps={{
                    "aria-labelledby": id,
                }}
            >
                <MenuItem onClick={copyMainTable}>Main Table</MenuItem>
                <MenuItem onClick={copyComparisonTable}>
                    Comparison Table
                </MenuItem>
            </Menu>
        </div>
    )
}

interface TopBarProps {
    onRunClick: () => void
    onCompareClick: () => void
    compareButtonLabel: string
}

export default function TopBar(
    props: PropsWithChildren<TopBarProps & CopyRequestToClipboardMenuProps>,
) {
    const scrollTarget = useRef()
    const trigger = useScrollTrigger({
        target: scrollTarget.current,
    })

    const colorMode = useColorMode()
    const theme = useTheme()

    return (
        <Box
            sx={{ width: "100%", height: "100%", overflow: "auto" }}
            ref={scrollTarget}
        >
            <Slide appear={false} direction="down" in={!trigger}>
                <AppBar position="sticky">
                    <Toolbar
                        sx={{
                            display: "flex",
                            justifyContent: "space-between",
                            alignItems: "center",
                        }}
                    >
                        <Box sx={{ display: "flex" }}>
                            <Button
                                sx={{ mr: 2 }}
                                startIcon={<PlayArrowIcon />}
                                color="inherit"
                                onClick={props.onRunClick}
                            >
                                Run
                            </Button>
                            <Button
                                sx={{ mr: 2 }}
                                startIcon={<CompareIcon />}
                                color="inherit"
                                onClick={props.onCompareClick}
                            >
                                {props.compareButtonLabel}
                            </Button>
                            <CopyRequestToClipboardMenu
                                copyComparisonTable={props.copyComparisonTable}
                                copyMainTable={props.copyMainTable}
                            />
                        </Box>

                        <IconButton
                            sx={{ ml: 1 }}
                            onClick={colorMode.toggleColorMode}
                            color="inherit"
                        >
                            {theme.palette.mode === "dark" ? (
                                <Brightness7Icon />
                            ) : (
                                <Brightness4Icon />
                            )}
                        </IconButton>
                    </Toolbar>
                </AppBar>
            </Slide>
            {props.children}
        </Box>
    )
}

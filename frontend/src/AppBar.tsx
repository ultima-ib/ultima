import { AppBar, Box, Toolbar, Slide, Button, IconButton } from "@mui/material"
import useScrollTrigger from "@mui/material/useScrollTrigger"
import { PropsWithChildren, useRef } from "react"
import { useTheme } from "@mui/material/styles"
import PlayArrowIcon from "@mui/icons-material/PlayArrow"
import Brightness4Icon from "@mui/icons-material/Brightness4"
import Brightness7Icon from "@mui/icons-material/Brightness7"
import { useColorMode } from "./App"

interface TopBarProps {
    onRunClick: () => void
}

export default function TopBar(props: PropsWithChildren<TopBarProps>) {
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
                        <Button
                            sx={{ mr: 2 }}
                            startIcon={<PlayArrowIcon />}
                            color="inherit"
                            onClick={props.onRunClick}
                        >
                            Run
                        </Button>
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

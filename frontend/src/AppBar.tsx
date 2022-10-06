import {AppBar, Box, Toolbar, Slide, Button} from '@mui/material'
import useScrollTrigger from '@mui/material/useScrollTrigger';
import {PropsWithChildren, useRef} from "react";
import PlayArrowIcon from '@mui/icons-material/PlayArrow';

interface TopBarProps {
    onRunClick: () => void
}

export default function TopBar(props: PropsWithChildren<TopBarProps>) {
    const scrollTarget = useRef()
    const trigger = useScrollTrigger({
        target: scrollTarget.current
    });

    return (
        <Box sx={{width: '100%', height: '100%', overflow: 'auto'}} ref={scrollTarget}>
            <Slide appear={false} direction="down" in={!trigger}>
                <AppBar position='sticky'>
                    <Toolbar>
                        <Button sx={{mr: 2}} startIcon={<PlayArrowIcon/>} color='inherit' onClick={props.onRunClick}>
                            Run
                        </Button>
                    </Toolbar>
                </AppBar>
            </Slide>
            {props.children}
        </Box>
    );
}

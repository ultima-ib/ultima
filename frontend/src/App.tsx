import Aside from "./aside";
import CssBaseline from '@mui/material/CssBaseline';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import TopBar from "./AppBar";
import {Box} from "@mui/material";

const darkTheme = createTheme({
    palette: {
        mode: 'dark',
    },
});


function App() {
    return (
        <ThemeProvider theme={darkTheme}>
            <style dangerouslySetInnerHTML={{ __html: "html { --color: rgba(255, 255, 255, 0.5); }"}}>
            </style>
            <CssBaseline>
                <Aside />
            </CssBaseline>
        </ThemeProvider>
    )
}

export default App

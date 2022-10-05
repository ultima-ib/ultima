import {
    ChangeEvent,
    PropsWithChildren,
    Suspense,
    SyntheticEvent,
    useDeferredValue,
    useEffect,
    useMemo,
    useState
} from 'react';
import type {DraggableLocation, DropResult} from '@hello-pangea/dnd';
import {DragDropContext} from '@hello-pangea/dnd';
import {reorderQuoteMap} from './reorder';
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
    TextField
} from "@mui/material";
import QuoteList from "./list";
import Title from "./Title";
import {Filters} from "./Filters";
import type {DataSet} from "./types";
import Agg from "./AggTypes";
import {InputStateUpdate, useInputs} from "./InputStateContext";
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import {Resizable as ReResizable} from "re-resizable";
import * as lunr from 'lunr'
import DeleteIcon from '@mui/icons-material/Close';

const ResizeHandle = () => {
    return <div
        style={{
            background: 'rgba(0, 0, 0, 0.3)',
            height: '100%',
            width: '2px',
        }}
    />
}

const Resizable = (props: PropsWithChildren) => (
    <ReResizable
        handleComponent={{
            right: <ResizeHandle/>
        }}
        minWidth='300px'
        defaultSize={{width: '35%', height: '100%'}}
        enable={{
            top: false,
            right: true,
            bottom: false,
            left: false,
            topRight: false,
            bottomRight: false,
            bottomLeft: false,
            topLeft: false
        }}
        style={{
            display: 'flex', gap: '1em', marginRight: '0.5em', minWidth: '300px'
        }}
    >
        {props.children}
    </ReResizable>
)

interface TabPanelProps extends BoxProps {
    index: number;
    value: number;
}

function TabPanel(props: TabPanelProps) {
    const {children, value, index, ...other} = props;

    return (
        <Box
            role="tabPanel"
            hidden={value !== index}
            id={`tabPanel-${index}`}
            aria-labelledby={`simple-tab-${index}`}
            {...other}
        >
            {value === index && children}
        </Box>
    );
}

function a11yProps(index: number) {
    return {
        id: `tab-${index}`,
        'aria-controls': `tabPanel-${index}`,
    };
}


interface ColumnProps extends StackProps {
    title?: string;
    fields: string[];
    listId: string,
    extras?: any
    onListItemClick?: (field: string) => void
}

export function Column({title, fields, listId, height, extras, onListItemClick, ...stack}: ColumnProps) {
    return (
        <Stack spacing={2} alignItems='center' {...stack}>
            {title && <Title content={title}/>}
            <QuoteList
                listId={listId}
                listType="QUOTE"
                fields={fields}
                extras={extras}
                onListItemClick={onListItemClick}
            />
        </Stack>
    );
}

const Accordion = ({
                       title,
                       children,
                       hideExpandButton,
                       ...rest
                   }: AccordionProps & { title: string, hideExpandButton?: boolean }) => (
    <MuiAccordion {...rest}>
        <AccordionSummary expandIcon={!hideExpandButton && <ExpandMoreIcon/>}>
            {title}
        </AccordionSummary>
        <AccordionDetails sx={{minHeight: '100px'}}>
            {children}
        </AccordionDetails>
    </MuiAccordion>
)

const AccordionColumn = ({title, ...rest}: ColumnProps) => {
    return (
        <Accordion title={title ?? ''}>
            <Column
                {...rest}
            />
        </Accordion>
    )
}

const SearchBox = (props: { onChange: (text: string) => void }) => {
    const [searchText, setSearchText] = useState('');
    const onSearchTextChange = (event: ChangeEvent<HTMLInputElement>) => {
        setSearchText(event.target.value);
    }
    const deferredSearchText = useDeferredValue(searchText)
    useEffect(() => {
        props.onChange(deferredSearchText)
    }, [deferredSearchText]);

    return <TextField value={searchText} onChange={onSearchTextChange} label="Search" sx={{my: 1, mx: 1}}
                      variant='filled'></TextField>
}

const DeleteButton = (props: { field: string, from: keyof Omit<DataSet, 'calcParams'>}) => {
    const inputs = useInputs();

    const onDelete = () => {
        const returnTo = props.from === 'measuresSelected' ? 'measures' : 'fields'
        const fromList = inputs.dataSet[props.from].filter(it => it !== props.field);
        const orgList = inputs.dataSet[returnTo];
        const toList = orgList.includes(props.field) ? orgList : [...orgList, props.field]

        inputs.dispatcher({
            type: InputStateUpdate.DataSet,
            data: {
                dataSet: {
                    [props.from]: fromList,
                    [returnTo]: toList
                }
            }
        })
    }

    return (
        <IconButton onClick={onDelete}>
            <DeleteIcon/>
        </IconButton>
    )
}

const FcBoard = (props: {
    onCalcParamsChange: (name: string, value: string) => void
}) => {
    const inputs = useInputs();
    const columns = inputs.dataSet;
    const onDragEnd = (result: DropResult): void => {
        const source: DraggableLocation = result.source;
        // dragged nowhere, bail
        if (result.destination === null) {
            return;
        }
        const destination: DraggableLocation = result.destination;

        // did not move anywhere - can bail early
        if (source.droppableId === destination.droppableId && source.index === destination.index) {
            return;
        }

        if (destination.droppableId === "fields" || destination.droppableId === "measures") {
            return
        }
        const data = reorderQuoteMap({
            quoteMap: columns,
            source,
            destination,
        });

        inputs.dispatcher({
            type: InputStateUpdate.DataSet,
            data: {
                dataSet: {
                    ...inputs.dataSet,
                    ...data
                }
            }
        })
    };

    const [activeTab, setActiveTab] = useState(0);

    const [searchValue, setSearchValue] = useState<string | undefined>(undefined);

    const doSearch = (orElse: string[]) => {
        if (searchValue) {
            const results = orElse.filter(it => it.toLowerCase().includes(searchValue.toLowerCase()))
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
        setActiveTab(newValue);
    };

    const addToList = (list: keyof DataSet, what: string) => {
        inputs.dispatcher({
            type: InputStateUpdate.DataSet,
            data: {
                // @ts-expect-error mismatched signature
                dataSet: {
                    [list]: [...columns[list], what]
                }
            }
        })
    }
    return (
        <DragDropContext onDragEnd={onDragEnd}>
            <Resizable>
                <Stack sx={{width: '40%'}}>
                    <SearchBox onChange={v => setSearchValue(v)}/>
                    <Column
                        title="Measures"
                        fields={doSearch(columns.measures)}
                        listId='measures'
                        sx={{height: '45%'}}
                        onListItemClick={(field) => {
                            console.log('measures: clicked on', field)
                            addToList('measuresSelected', field)
                        }}
                    />
                    <Column
                        title="Fields"
                        fields={doSearch(columns.fields)}
                        listId='fields'
                        sx={{height: '45%'}}
                        onListItemClick={(field) => {
                            console.log('fields: clicked on', field)
                            addToList('groupby', field)
                        }}
                    />
                </Stack>
                <Stack sx={{width: '60%', height: '100%'}}>
                    <Box sx={{borderBottom: 1, borderColor: 'divider'}}>
                        <Tabs value={activeTab} onChange={handleActiveTabChange}
                              aria-label="basic tabs example">
                            <Tab label="Aggregate" {...a11yProps(0)} />
                            <Tab label="Params" {...a11yProps(1)} />
                        </Tabs>
                    </Box>
                    <TabPanel value={activeTab} index={0} sx={{height: '100%'}}>
                        <AccordionColumn
                            title="Group By"
                            fields={columns.groupby ?? []}
                            listId='groupby'
                            sx={{height: '20%'}}
                            extras={({field}) => <DeleteButton field={field} from='groupby' />}
                        />
                        <AccordionColumn
                            title="Overwrites"
                            fields={columns.overwrites ?? []}
                            listId='overwrites'
                            sx={{height: '20%'}}
                            extras={({field}) => <DeleteButton field={field} from='overwrites' />}
                        />
                        <AccordionColumn
                            title="Measures"
                            fields={columns.measuresSelected ?? []}
                            listId='measuresSelected'
                            sx={{height: '20%'}}
                            extras={({field}: { field: string }) => (<>
                                {inputs.canMeasureBeAggregated(field) &&
                                    <Suspense>
                                        <Agg field={field}/>
                                    </Suspense>
                                }
                                <DeleteButton field={field} from='measuresSelected'/>
                            </>)}
                        />
                        <Accordion title="Filters" expanded hideExpandButton>
                            <Filters/>
                        </Accordion>
                    </TabPanel>
                    <TabPanel value={activeTab} index={1} sx={{height: '100%'}}>
                        <Box>
                            <FormControlLabel
                                control={
                                    <Checkbox
                                        checked={inputs.hideZeros}
                                        onChange={(e) => inputs.dispatcher({
                                            type: InputStateUpdate.HideZeros,
                                            data: {hideZeros: e.target.checked}
                                        })}
                                    />
                                }
                                label="Hide Zeros"
                            />

                            <FormControlLabel
                                control={
                                    <Checkbox
                                        checked={inputs.totals}
                                        onChange={(e) => inputs.dispatcher({
                                            type: InputStateUpdate.Total,
                                            data: {totals: e.target.checked}
                                        })}
                                    />
                                }
                                label="Totals"
                            />
                        </Box>
                        <Box sx={{overflowY: 'scroll', maxHeight: '80vh'}}>
                            {
                                columns.calcParams.map((it) => (
                                    <TextField
                                        key={it.name}
                                        label={it.name}
                                        defaultValue={it.defaultValue}
                                        helperText={it.helperText}
                                        onChange={(e) => {
                                            props.onCalcParamsChange?.(it.name, e.target.value)
                                        }}
                                        variant="filled"
                                    />
                                ))
                            }
                        </Box>
                    </TabPanel>
                </Stack>
            </Resizable>
        </DragDropContext>
    )
}
export default FcBoard

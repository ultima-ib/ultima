import React, {Dispatch, Suspense, MutableRefObject, SetStateAction} from 'react';
import type {DraggableLocation, DropResult} from '@hello-pangea/dnd';
import {DragDropContext} from '@hello-pangea/dnd';
import type {DataSet} from './types';
import {reorderQuoteMap} from './reorder';
import {Box, Checkbox, FormControlLabel, Stack} from "@mui/material";
import QuoteList from "./list";
import Title from "./Title";
import {Filters} from "./Filters";
import {Filter} from "./types";
import Agg from "./AggTypes";

interface ColumnProps {
  title: string;
  fields: string[];
  listId: string,
  height: string,
  extras?: any
}

export function Column({ title, fields, listId, height, extras }: ColumnProps) {
  return (
      <Stack spacing={2} alignItems='center'>
        <Title content={title} />
        <QuoteList
            listId={listId}
            listType="QUOTE"
            fields={fields}
            height={height}
            internalScroll={true}
            extras={extras}
        />
      </Stack>
  );
}

const BooleanOption = (props: {
  state: [boolean, Dispatch<SetStateAction<boolean>>]
  label: string
}) => {
  const [checked, setChecked] = props.state

  const handleChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    setChecked(event.target.checked);
  };

  return (
      <FormControlLabel control={<Checkbox checked={checked} onChange={handleChange}/>} label={props.label}/>
  )
}

const SearchBox = () => {
  return <></>
}

const PresetSelector = () => {
  return <></>
}

interface Props {
  dataSet: [DataSet, Dispatch<SetStateAction<DataSet>>];
  filters: MutableRefObject<{ [p: number]: { [p: number]: Filter } }>
  hideZeros: [boolean, Dispatch<SetStateAction<boolean>>];
  totals: [boolean, Dispatch<SetStateAction<boolean>>];
  withScrollableColumns?: boolean;
  isCombineEnabled?: boolean;
  containerHeight?: string;
  useClone?: boolean;
}

const FcBoard = (props: Props) => {
  const [columns, setColumns] = props.dataSet

  const onDragEnd = (result: DropResult): void => {
    const source: DraggableLocation = result.source;
    if (result.destination === null) {
      if (!(source.droppableId === "fields" || source.droppableId === "measures")) {
        setColumns((prev: any) => {
          const list: any[] = prev[source.droppableId];
          list.splice(source.index, 1)
          return {
            ...prev,
            [source.droppableId]: list
          }
        })
      }
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
    setColumns(data)
  };

  return (
      <>
        <DragDropContext onDragEnd={onDragEnd}>
          <Box component='aside' sx={{display: 'flex', gap: 2, width: '35%'}}>
            <Stack  sx={{width: '40%'}}>
              <SearchBox />
              <Column
                  title="Measures"
                  fields={columns.measures} // apply search
                  listId='measures'
                  height={'300px'}
              />
              <Column
                  title="Fields"
                  fields={columns.fields} // apply search
                  listId='fields'
                  height={'300px'}
              />
            </Stack>
            <Stack sx={{width: '60%'}}>
              <PresetSelector />
              <Column
                  title="Group By"
                  fields={columns.groupby ?? []}
                  listId='groupby'
                  height={'7rem'}
              />
              <Column
                  title="Overwrites"
                  fields={columns.overwrites ?? []}
                  listId='overwrites'
                  height={'7rem'}
              />
              <Column
                  title="Measures"
                  fields={columns.measuresSelected ?? []}
                  listId='measuresSelected'
                  height={'7rem'}
                  extras={({field}: { field: string }) => (columns.canBeAggregated(field) ? (
                      <Suspense>
                        <Agg field={field} />
                      </Suspense>
                  ) : (<></>)) }
              />
              <Filters
                  filters={props.filters}
                  fields={columns.fields}
                  sx={{ height:'7rem' }}
              />
              <Box>
                <BooleanOption state={props.hideZeros} label="Hide Zeros" />
                <BooleanOption state={props.totals} label="Totals" />
              </Box>
            </Stack>
          </Box>
        </DragDropContext>
      </>
  )
}
export default FcBoard

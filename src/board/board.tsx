import React, {useState} from 'react';
import type {DropResult, DraggableLocation} from '@hello-pangea/dnd';
import { DragDropContext } from '@hello-pangea/dnd';
import type { QuoteMap, Quote } from './types';
import { reorderQuoteMap } from './reorder';
import {Box, Stack, Typography} from "@mui/material";
import QuoteList from "./quote-list";

interface ColumnProps {
  title: string;
  quotes: Quote[];
}

export function Column({ title, quotes }: ColumnProps) {
  return (
      <Stack spacing={2}>
        <Typography variant='h6'>{title ?? 'no title'}</Typography>
        <QuoteList
            listId={title}
            listType="QUOTE"
            quotes={quotes}
            internalScroll={true}
        />
      </Stack>
  );
}


interface Props {
  initial: QuoteMap;
  withScrollableColumns?: boolean;
  isCombineEnabled?: boolean;
  containerHeight?: string;
  useClone?: boolean;
}

const FcBoard = (props: Props) => {
  const [columns, setColumns] = useState(props.initial)

  const onDragEnd = (result: DropResult): void => {
    if (!result.destination) {
      return;
    }

    const source: DraggableLocation = result.source;
    const destination: DraggableLocation = result.destination;

    // did not move anywhere - can bail early
    if (source.droppableId === destination.droppableId && source.index === destination.index) {
      return;
    }

    const data = reorderQuoteMap({
      quoteMap: columns,
      source,
      destination,
    });
    console.log(result)

    setColumns(data.quoteMap)
  };

  return (
      <>
        <DragDropContext onDragEnd={onDragEnd}>
          <Box>
            {Object.entries(columns).map(([key, quotes]) => (
                <Column
                    key={key}
                    title={key}
                    quotes={quotes}
                />
            ))}
          </Box>
        </DragDropContext>
      </>

  )
}
export default FcBoard

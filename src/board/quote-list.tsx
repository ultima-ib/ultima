import React, { CSSProperties, ReactElement } from 'react';
import { Droppable, Draggable } from '@hello-pangea/dnd';
import type {
  DroppableProvided,
  DroppableStateSnapshot,
  DraggableProvided,
  DraggableStateSnapshot,
} from '@hello-pangea/dnd';
import type { Quote } from './types';
import {Avatar, Box, List, ListItem, ListItemAvatar, ListItemButton, ListItemText} from "@mui/material";

interface QuoteItemProps {
  quote: Quote;
  isDragging: boolean;
  provided: DraggableProvided;
}

function QuoteItem({ quote, provided }: QuoteItemProps) {
  return (
      <ListItem
          ref={provided.innerRef}
          {...provided.draggableProps}
          {...provided.dragHandleProps}
      >
        <ListItemButton>
          <ListItemAvatar>
            <Avatar src={quote.author.avatarUrl} alt={quote.author.name} />
          </ListItemAvatar>
          <ListItemText>
            {quote.content}
          </ListItemText>
        </ListItemButton>
      </ListItem>
  )
}


interface Props {
  listId?: string;
  listType?: string;
  quotes: Quote[];
  title?: string;
  internalScroll?: boolean;
  scrollContainerStyle?: CSSProperties;
  isDropDisabled?: boolean;
  style?: CSSProperties;
  // may not be provided - and might be null
  ignoreContainerClipping?: boolean;
  useClone?: boolean;
}

interface QuoteListProps {
  quotes: Quote[];
}

function InnerQuoteList(props: QuoteListProps): ReactElement {
  return (
    <>
      {props.quotes.map((quote: Quote, index: number) => (
        <Draggable key={quote.id} draggableId={quote.id} index={index}>
          {(
            dragProvided: DraggableProvided,
            dragSnapshot: DraggableStateSnapshot,
          ) => (
            <QuoteItem
              key={quote.id}
              quote={quote}
              isDragging={dragSnapshot.isDragging}
              provided={dragProvided}
            />
          )}
        </Draggable>
      ))}
    </>
  );
}

interface InnerListProps {
  dropProvided: DroppableProvided;
  quotes: Quote[];
  title: string | undefined | null;
}

function InnerList(props: InnerListProps) {
  const { quotes, dropProvided } = props;
  return (
      <Box ref={dropProvided.innerRef}>
        <List>
          <InnerQuoteList quotes={quotes} />
        </List>
        {dropProvided.placeholder}
      </Box>
  );
}

export default function QuoteList(props: Props): ReactElement {
  const {
    ignoreContainerClipping,
    isDropDisabled,
    listId = 'LIST',
    listType,
    quotes,
    title,
  } = props;

  return (
    <Droppable
      droppableId={listId}
      type={listType}
      ignoreContainerClipping={ignoreContainerClipping}
      isDropDisabled={isDropDisabled}
      renderClone={(provided, snapshot, descriptor) => (
          <QuoteItem
              quote={quotes[descriptor.source.index]}
              provided={provided}
              isDragging={snapshot.isDragging}
          />
      )}
    >
      {(
        dropProvided: DroppableProvided,
        dropSnapshot: DroppableStateSnapshot,
      ) => (
        <Box

          {...dropProvided.droppableProps}
        >
          <InnerList
              quotes={quotes}
              title={title}
              dropProvided={dropProvided}
          />
        </Box>
      )}
    </Droppable>
  );
}

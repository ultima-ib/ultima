import React, { Component, ReactElement } from 'react';
import styled from '@emotion/styled';
import { Global, css } from '@emotion/react';
import type {
  DropResult,
  DraggableLocation,
  DroppableProvided,
} from '@hello-pangea/dnd';
import { DragDropContext, Droppable } from '@hello-pangea/dnd';
import type { QuoteMap, Quote } from './types';
import Column from './column';
import reorder, { reorderQuoteMap } from './reorder';
import * as colors from './colors';


interface ParentContainerProps {
  height: string;
}

const ParentContainer = styled.div<ParentContainerProps>`
  height: ${({ height }) => height};
  overflow-x: hidden;
  overflow-y: auto;
`;

const Container = styled.div`
  background-color: ${colors.B100};
  min-height: 100vh;
  /* like display:flex but will allow bleeding over the window width */
  min-width: 100vw;
  display: inline-flex;
`;

interface Props {
  initial: QuoteMap;
  withScrollableColumns?: boolean;
  isCombineEnabled?: boolean;
  containerHeight?: string;
  useClone?: boolean;
}

interface State {
  columns: QuoteMap;
  ordered: string[];
}



export default class Board extends Component<Props, State> {
  /* eslint-disable react/sort-comp */
  static defaultProps = {
    isCombineEnabled: false,
  };

  state: State = {
    columns: this.props.initial,
    ordered: Object.keys(this.props.initial),
  };

  onDragEnd = (result: DropResult): void => {
    if (!result.destination) {
      return;
    }

    const source: DraggableLocation = result.source;
    const destination: DraggableLocation = result.destination;

    // did not move anywhere - can bail early
    if (
      source.droppableId === destination.droppableId &&
      source.index === destination.index
    ) {
      return;
    }

    const data = reorderQuoteMap({
      quoteMap: this.state.columns,
      source,
      destination,
    });
    console.log(data.quoteMap)

    this.setState({
      columns: data.quoteMap,
    });
  };

  render(): ReactElement {
    const columns: QuoteMap = this.state.columns;
    const ordered: string[] = this.state.ordered;
    const {
      containerHeight,
      useClone,
      withScrollableColumns,
    } = this.props;

    const board = <Container>
      {ordered.map((key: string, index: number) => (
          <Column
              key={key}
              index={index}
              title={key}
              quotes={columns[key]}
              isScrollable={withScrollableColumns}
              useClone={useClone}
          />
      ))}
    </Container>

    return (
      <React.Fragment>
        <DragDropContext onDragEnd={this.onDragEnd}>
          {containerHeight ? (
            <ParentContainer height={containerHeight}>{board}</ParentContainer>
          ) : (
            board
          )}
        </DragDropContext>
      </React.Fragment>
    );
  }
}

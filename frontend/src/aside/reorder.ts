import type { DraggableLocation } from '@hello-pangea/dnd';
import type {DataSet as Data} from './types';

type DataSet = Omit<Data, 'calcParams'>

// a little function to help us with reordering the result
function reorder<TItem>(
  list: TItem[],
  startIndex: number,
  endIndex: number,
): TItem[] {
  const result = [...list];
  const [removed] = result.splice(startIndex, 1);
  result.splice(endIndex, 0, removed);

  return result;
}

export default reorder;

export const reorderQuoteMap = ({
  quoteMap,
  source,
  destination,
}: {
  quoteMap: DataSet;
  source: DraggableLocation;
  destination: DraggableLocation;
}) => {
  const current: string[] = [...(quoteMap[source.droppableId as keyof DataSet] ?? [])];
  const next: string[] = [...(quoteMap[destination.droppableId as keyof DataSet] ?? [])];
  const target = current[source.index];
  console.log({current, next})
  // moving to same list
  if (source.droppableId === destination.droppableId) {
    const reordered: string[] = reorder(
      current,
      source.index,
      destination.index,
    );
    return  {
      ...quoteMap,
      [source.droppableId]: reordered,
    }
  }

  if (
      (source.droppableId === 'measures' && destination.droppableId !== 'measuresSelected') ||
      (source.droppableId === 'fields' && destination.droppableId === 'measuresSelected')
  ) {
    // impossible
    return quoteMap
  }

  // remove from original
    current.splice(source.index, 1);

  // insert into next
  next.splice(destination.index, 0, target);

  return {
    ...quoteMap,
    [source.droppableId]: current,
    [destination.droppableId]: next,
  }
};

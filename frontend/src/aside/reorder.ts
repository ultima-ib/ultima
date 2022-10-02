import type { DraggableLocation } from '@hello-pangea/dnd';
import type {DataSet} from './types';

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

  console.log({
    source: source.droppableId,
    destination: destination.droppableId
  })
  if (
      (source.droppableId === 'measures' && destination.droppableId !== 'measuresSelected') ||
      (source.droppableId === 'fields' && destination.droppableId === 'measuresSelected')
  ) {
    // impossible
    return quoteMap
  }

  // remove from original
  // if (source.droppableId !== 'fields' && source.droppableId !== 'measures') {
    current.splice(source.index, 1);
  // }
  // insert into next
  next.splice(destination.index, 0, target);

  return {
    ...quoteMap,
    [source.droppableId]: current,
    [destination.droppableId]: next,
  }
};

interface List<T> {
  id: string;
  values: T[];
}

interface MoveBetweenArgs<T> {
  list1: List<T>;
  list2: List<T>;
  source: DraggableLocation;
  destination: DraggableLocation;
}

interface MoveBetweenResult<T> {
  list1: List<T>;
  list2: List<T>;
}

export function moveBetween<T>({
  list1,
  list2,
  source,
  destination,
}: MoveBetweenArgs<T>): MoveBetweenResult<T> {
  const newFirst = [...list1.values];
  const newSecond = [...list2.values];

  const moveFrom = source.droppableId === list1.id ? newFirst : newSecond;
  const moveTo = moveFrom === newFirst ? newSecond : newFirst;

  const [moved] = moveFrom.splice(source.index, 1);
  moveTo.splice(destination.index, 0, moved);

  return {
    list1: {
      ...list1,
      values: newFirst,
    },
    list2: {
      ...list2,
      values: newSecond,
    },
  };
}

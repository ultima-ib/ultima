import { Filter } from "../aside/types"

export type Filters = Record<number, Record<number, Filter>>

type State = Record<number, Record<number, object>>

export enum ActionType {
    NewRoot,
    NewChild,
    RemoveRoot,
    RemoveChild,
    Update,
    Set,
}

export interface Action {
    type: ActionType
}

export interface NewRoot extends Action {
    index: number
}

export interface NewChild extends Action {
    andIndex: number
    index: number
}

export interface Update extends NewChild, Filter {}

export interface Set extends Action {
    filters: Filters
}

const EMPTY_FILTER: State = {}

export function reducer(
    prevState: State,
    action: NewRoot | NewChild | Update | Set,
): State {
    switch (action.type) {
        case ActionType.NewRoot: {
            const data = action as NewRoot
            return {
                ...prevState,
                [data.index]: EMPTY_FILTER,
            }
        }
        case ActionType.RemoveRoot: {
            const data = action as NewRoot
            if (Object.keys(prevState[data.index]).length === 0) {
                const copy = { ...prevState }
                // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
                delete copy[data.index]
                return copy
            }
            return prevState
        }
        case ActionType.NewChild: {
            const data = action as NewChild
            return {
                ...prevState,
                [data.andIndex]: {
                    ...prevState[data.andIndex],
                    [data.index]: EMPTY_FILTER,
                },
            }
        }
        case ActionType.RemoveChild: {
            const data = action as NewChild
            const copy = { ...prevState[data.andIndex] }
            // eslint-disable-next-line @typescript-eslint/no-dynamic-delete
            delete copy[data.index]
            return {
                ...prevState,
                [data.andIndex]: copy,
            }
        }
        case ActionType.Update: {
            const { andIndex, index, ...data } = action as Update
            return {
                ...prevState,
                [andIndex]: {
                    ...prevState[andIndex],
                    [index]: data,
                },
            }
        }
        case ActionType.Set: {
            const data = action as Set
            return data.filters
        }
    }
    throw Error("unreachable")
}

export type ReducerDispatch = (a: NewRoot | NewChild | Update | Set) => void

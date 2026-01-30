import { OrderFilled as OrderFilledEvent } from "../generated/CTFExchange/CTFExchange"
import { OrderFilled } from "../generated/schema"

export function handleOrderFilled(event: OrderFilledEvent): void {
  // Unique ID: Transaction Hash + Log Index
  let entity = new OrderFilled(
    event.transaction.hash.concatI32(event.logIndex.toI32())
  )

  // Save the data
  entity.maker = event.params.maker
  entity.taker = event.params.taker
  entity.makerAmount = event.params.makerAmount
  entity.takerAmount = event.params.takerAmount
  entity.timestamp = event.block.timestamp

  entity.save()
}

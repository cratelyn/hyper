# HIP-0002: `Body::poll_progress()`

- Authors: katelyn martin @cratelyn
- Created: 26-10-12
- PR: TBD

## Summary

(The tl;dr. Concise statement of the reason for the document and
recommendation.)

## Tenets

(List of tenets, values, guiding principles that are used to evaluate the
proposal.)

## Motivation

`http_body::Body` is a trait representing the streaming body of a request or
response. This trait is used throughout not only `hyper`, but also much of the
Rust ecosystem as the foundational interface representing the stream of data
in a message payload.

This trait is designed for use in asynchronous applications running inside of
runtimes like `tokio`, a common pattern within the Rust ecosystem for
performantly dealing with asynchronous I/O.

### Bodies are streams

Today, the signature of the `Body` trait is shown below. For the purpose of
this discussion we will elide the "hint" methods `Body::is_end_stream()` and
`Body::size_hint()`.

```rust
pub trait Body {
    type Data: Buf;
    type Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>>;
}
```

This is worth comparing to the signature of `std::async_iter::AsyncIterator`
(rust-lang/rust#79024), which defines the concept of an asynchronous
iterator.

```rust
pub trait AsyncIterator {
    type Item;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>>;
}
```

Notice how `Body::poll_frame()` and `AsyncIterator::poll_next()` share the same
`Pin<&mut Self>` receiver, and both require a mutable reference to the
`std::task::Context` of an asynchronous task. Notice that both return a
`Poll<Option<T>>` value, but `Body::poll_frame()` differs in two ways: `Body`
presents a _fallible_ interface, and it represents each successful frame as a
`http_body::Frame<T>` to account for HTTP trailers.

So, `http_body::Body` is a fallible stream intended for use in HTTP clients
and servers. This may sound obvious to say, but highlighting that this
interface is a sort of domain-specific `AsyncIterator` is worth drawing
attention to before proceeding further.

### Demo:

TODO: spin up a small program demonstrating the issue.

---

Oxide's RFC 0609 describes a category of bug called "Futurelock". This is,
broadly, "_a type of deadlock where a resource owned by Future `A` is required
for another Future `B` to proceed, while the Task responsible for both Futures
is no longer polling `A`._"

"Barbara battles buffered streams"

## Recommendation

### User Experience

### Implementation

### Testing Plan

(Optional: are there any special considerations for testing required?)

### Security Considerations

(What security risks might exist for the recommendation? Can we reduce them?
What should users consider when using it?)

## Alternatives

(List alternatives considered, and especially why they were not recommended.
This portion of "documenting the why" helps Future Us to see if anything has
changed since last considered.)

## Unresolved Questions

## References

- <https://without.boats/blog/poll-progress/>
- <https://github.com/rust-lang/rust/issues/79024>
- <https://tmandry.gitlab.io/blog/posts/for-await-buffered-streams/>
- <https://docs.rs/futures/latest/futures/prelude/trait.Stream.html>
- <https://github.com/rust-lang/wg-async/blob/master/src/vision/submitted_stories/status_quo/barbara_battles_buffered_streams.md>
- <https://rfd.shared.oxide.computer/rfd/0609>

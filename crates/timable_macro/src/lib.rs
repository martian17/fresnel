// extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};



#[proc_macro_derive(Timable)]
pub fn derive_timable(item: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(item as DeriveInput);
    let name = ast.ident;
    let (impl_generics, type_generics, where_clause) = ast.generics.split_for_impl();

    let code = quote! {
        // Hard coded trait location in the parent crate
        impl #impl_generics crate::util::core::Timable for #name #type_generics #where_clause {}
    };
    code.into()
}
